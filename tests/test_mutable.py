"""`WorkingRecord`: staging, the edit operations, commit.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
"""

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
from tests.fixtures import export_network, schema

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
    `p_nom` lives in `dims/entity_type/`, so `inputs/` alone would not
    show what the record resolves to.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return PyPSA.build(revision.record).c[ctype].static[attribute].to_dict()


# -- the protocol (https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit) ----------------------------------------------------


def test_a_mutable_record_reads_as_a_record(staged):
    """Editable *and* readable: the pending edits are a layer, so reads compose.

    The load-bearing half of `WorkingRecord`: it satisfies `Record`, so what
    it reads is the data with its pending edits applied and it can be handed
    to anything that only knows how to read.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
    """
    assert isinstance(staged, Record)


# -- value forms (https://energy-models.github.io/datarecord/design/working-record/#set) -----------------------------------------------------


def test_scalar_applies_to_every_name():
    names, values, dims = normalise_value(150.0, ["wind1", "wind2"])
    assert (names, values, dims) == (["wind1", "wind2"], [150.0, 150.0], {})


def test_a_sequence_is_positional():
    names, values, _ = normalise_value([150.0, 80.0], ["wind1", "wind2"])
    assert names is not None
    assert dict(zip(names, values, strict=True)) == {"wind1": 150.0, "wind2": 80.0}


def test_a_mapping_supplies_its_own_names():
    names, values, _ = normalise_value({"wind1": 150.0, "wind2": 80.0}, None)
    assert names is not None
    assert dict(zip(names, values, strict=True)) == {"wind1": 150.0, "wind2": 80.0}


def test_a_series_indexed_by_names_is_per_name():
    series = pd.Series({"wind1": 1.0, "wind2": 2.0})
    names, values, dims = normalise_value(series, ["wind1", "wind2"])
    assert names is not None
    assert dict(zip(names, values, strict=True)) == {"wind1": 1.0, "wind2": 2.0}
    assert dims == {}


def test_a_series_indexed_by_an_axis_is_per_coordinate():
    """The same type read as a dim series - the caller says which, never the labels."""
    series = pd.Series({"2030-01-01": 0.4, "2030-01-02": 0.6})
    names, values, dims = normalise_value(series, None, indexed_by="snapshot")
    assert names is None
    assert values == [0.4, 0.6]
    assert dims == {"snapshot": ["2030-01-01", "2030-01-02"]}


def test_a_sequence_of_the_wrong_length_is_rejected():
    with pytest.raises(ValueError, match="2 names"):
        normalise_value([1.0, 2.0, 3.0], ["wind1", "wind2"])


def test_an_index_that_could_be_either_is_read_as_names(staged):
    """No membership test, so a label colliding with a name is not an ambiguity.

    `scenario` here has a label spelled like a component; with nothing said, the
    index is names - a call's meaning is its own, not the record's data's.
    """
    series = pd.Series({"wind1": 1.0})
    names, _, dims = normalise_value(series, ["wind1"])
    assert names == ["wind1"]
    assert dims == {}, "no axis claimed it"


def test_indexed_by_names_the_axis_a_series_index_holds(staged):
    """Said outright, since nothing about the labels themselves could say it."""
    series = pd.Series({pd.Timestamp("2015-01-01"): 0.4})
    staged.set("p_max_pu", series, entity=["Manchester Wind"], indexed_by="snapshot")
    assert "p_max_pu" in staged.attributes


def test_an_unnamed_series_index_holds_names(staged):
    """Not inferred from the labels, so an index saying nothing is names.

    Which then fails the member check rather than silently landing on an axis -
    a timestamp is no component.
    """
    series = pd.Series({"2030-01-01": 0.4})
    with pytest.raises(KeyError, match="no member row"):
        staged.set("p_max_pu", series, entity=["Manchester Wind"])


def test_the_series_index_name_says_what_it_holds(staged):
    """A caller who named the index has already said it; `indexed_by=` is spare."""
    index = pd.Index([pd.Timestamp("2015-01-01")], name="snapshot")
    staged.set("p_max_pu", pd.Series([0.4], index=index), entity=["Manchester Wind"])
    assert "p_max_pu" in staged.attributes


def test_indexed_by_must_be_a_coordinate_of_the_attribute(staged):
    series = pd.Series({"a": 1.0})
    with pytest.raises(ValueError, match="no coordinate of"):
        staged.set("p_nom", series, entity=["Manchester Wind"], indexed_by="snapshot")


# -- set (https://energy-models.github.io/datarecord/design/working-record/#set) -------------------------------------------------------------


def test_set_stages_without_writing(staged, root):
    """Staging is not a layer: the record reads the edit, the record does not."""
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    assert (
        dict(zip(rows["entity"], rows["value"], strict=True))["Manchester Wind"]
        == 150.0
    )
    # The record itself is untouched until commit.
    assert _static(root, "p_nom")["Manchester Wind"] != 150.0


def test_a_staged_edit_is_visible_through_the_record(staged):
    """A set of pending edits is a layer, so the read resolves over it.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = set(rows[rows["entity"] == "Manchester Wind"]["value"])
    assert got == {0.42}
    # Every other name still reads the base record's rows.
    assert set(rows["entity"]) > {"Manchester Wind"}


def test_two_lazy_reads_stay_bound_to_their_own_relations(staged, con):
    """A `Record` hands over unmaterialised frames, so two must not alias.

    The read path composes relations by replacement scan, which binds each one
    at build time. Registering them under a fixed catalog name instead would
    rebind on the second read and both frames would collapse onto the last
    one - the frames are lazy, so nothing forces the first before that happens.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    first = staged.attributes["p_max_pu"]

    staged.set("p_min_pu", 0.11, entity=["Manchester Wind"])
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
    """Two edits to one key collapse to the later one, by `_seq`.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 100.0, entity=["Manchester Wind"])
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    # Read back before commit: the collapse is what the read applies too, so
    # the second edit wins without the rows having been merged when staged.
    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    mine = rows[rows["entity"] == "Manchester Wind"]
    assert list(mine["value"]) == [150.0], "one row survives the collapse, the later"

    child = staged.commit(NewChild(root))
    assert _static(child, "p_nom")["Manchester Wind"] == 150.0


def test_set_over_several_names(staged, root):
    staged.set("p_nom", 150.0, entity=["Manchester Wind", "Norway Wind"])
    child = staged.commit(NewChild(root))

    got = _static(child, "p_nom")
    assert got["Manchester Wind"] == got["Norway Wind"] == 150.0


def test_set_rejects_an_unknown_name(staged):
    """A value for a name no layer declares would resolve to nothing.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    with pytest.raises(KeyError, match="Nope"):
        staged.set("p_nom", 1.0, entity=["Nope"])


def test_set_accepts_a_name_staged_by_add(staged, root):
    """`add` makes the name exist, so a value for it is no longer unknown.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    staged.add(GEN, pd.DataFrame([{"entity": "NewSolar", "carrier": "solar"}]))
    staged.set("p_nom", 7.0, entity=["NewSolar"])

    child = staged.commit(NewChild(root))
    assert _static(child, "p_nom")["NewSolar"] == 7.0


# -- the overlay does not overlap (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule, https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) -----------------------------


def test_a_broadcast_edit_displaces_the_whole_series(staged):
    """A staged NULL dim means "all values of that dim", so it replaces them.

    Rows never overlap within a record, so a broadcast edit and the base's
    per-snapshot rows cannot both survive - the edit covers every snapshot.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = rows[rows["entity"] == "Manchester Wind"]
    assert set(mine["value"]) == {0.42}
    assert mine["snapshot"].isna().all()


def test_a_pointwise_edit_keeps_the_rest_of_the_series(staged):
    """An edit naming a coordinate displaces that one only.

    Keying the overlay on the input key alone would drop the whole series here,
    since it excludes the dims an attribute is not owned per.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one, entity=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = rows[rows["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(got) == len(mine)
    assert got.iloc[0]["value"] == 0.123
    assert got.iloc[1:]["value"].tolist() == mine.iloc[1:]["value"].tolist()


def test_a_long_frame_naming_an_unknown_component_is_rejected(staged):
    """Validation applies to the frame form too: a typo is caught where it is typed.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(KeyError, match="Nope"):
        staged.set(
            "p_max_pu",
            pd.DataFrame([{"entity": "Nope", "value": 1.0}]),
        )


def test_an_expression_value_stages_the_whole_series(staged, root):
    """A derived edit covers every coordinate it read, not just one.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")

    staged.set("p_max_pu", nw.col("value") * 2, entity=["Manchester Wind"])
    read = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    read = read[read["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert read["value"].tolist() == (mine["value"] * 2).tolist(), (
        "every coordinate the expression read, not just one"
    )

    child = staged.commit(NewChild(root))
    got = child.record.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = got[got["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert got["value"].tolist() == (mine["value"] * 2).tolist()


def test_flags_report_a_dim_a_staged_edit_introduces(staged, ac_dc):
    """A staged row's dims join the flags, unioned with the base answer.

    `flags` is the one non-`Frames` member of `Record`, so the promise that a
    read reflects pending edits has to hold for it too - and it decides which
    container a consumer puts a value in (`PyPSA.build` splits static from
    series on exactly this). `marginal_cost` starts broadcast over `snapshot`
    and varying over nothing; a per-snapshot edit must add `snapshot` to
    `varies` while leaving `broadcast` alone, since the base's NULL-snapshot
    rows are still there.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """
    before = staged.flags(GEN)["marginal_cost"]
    assert "snapshot" not in before.varies
    assert "snapshot" in before.broadcast

    staged.set(
        "marginal_cost",
        pd.DataFrame(
            [
                {
                    "entity": "Manchester Wind",
                    "snapshot": ac_dc.snapshots[0],
                    "value": 7.5,
                }
            ]
        ),
        entity=["Manchester Wind"],
    )

    after = staged.flags(GEN)["marginal_cost"]
    assert "snapshot" in after.varies
    assert after.broadcast == before.broadcast


# -- value dtypes (https://energy-models.github.io/datarecord/design/record/#flags, https://energy-models.github.io/datarecord/design/schema/#attributespec) -----------------------------------------------


def test_a_non_float_attribute_stages_and_commits(staged, root):
    """`value` carries the attribute's declared dtype, not always `DOUBLE`.

    One staging table holds every attribute's values, so it stages `value` as
    text and casts to the declared dtype where the attribute is known - which
    is the point at which `inputs/<attr>.parquet` is per-attribute.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    amended = read_schema()
    amended.attributes["carrier"] = AttributeSpec(
        dtype=nw.String(), dims={"entity", "scenario"}
    )
    write_schema(amended)

    staged.set("carrier", "solar", entity=["Manchester Wind"])
    rows = staged.attributes["carrier"].collect().to_native().to_pandas()
    assert rows[rows["entity"] == "Manchester Wind"]["value"].tolist() == ["solar"]

    child = staged.commit(NewChild(root))
    got = child.record.attributes["carrier"].collect().to_native().to_pandas()
    assert got[got["entity"] == "Manchester Wind"]["value"].tolist() == ["solar"]


def test_a_float_attribute_stays_numeric(staged):
    """The cast is per attribute, so a `DOUBLE` one is not turned into text."""
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    assert rows["value"].dtype.kind == "f"
    assert 150.0 in set(rows["value"])


# -- ownership granularity (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override, https://energy-models.github.io/datarecord/design/working-record/#committing) -------------------------------------


def test_a_non_partial_axis_is_restated_whole(staged, root):
    """Touching one snapshot makes the layer own the whole series.

    `snapshot` is declared but not `partial`, so a layer cannot patch one hour
    and leave the rest to the parent: the coordinates the edit did not name
    would resolve to nothing rather than to the parent's value. So the commit
    reads the resolved series and writes it out complete - the one commit-time
    read of parent data.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    assert "snapshot" not in (staged.schema.partial or frozenset())
    # Owned per entity, since a layer patches one component without restating
    # the rest - but not per snapshot, which is the axis this test is about.
    assert "snapshot" not in staged.schema.owned_per("p_max_pu")

    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(mine) > 1
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    got = child.record.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = got[got["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(got) == len(mine)
    # The edit applied, and every other hour kept the parent's value.
    assert got.iloc[0]["value"] == 0.123
    assert got.iloc[1:]["value"].tolist() == mine.iloc[1:]["value"].tolist()


def test_the_restated_series_is_in_the_layer_itself(staged, root, con):
    """The layer carries the whole extent, not a parent lookup at read time.

    A patch layer holds only edits - except along an axis owned whole,
    where the completed series must be in the layer, since that is what makes
    this layer its owner.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"]
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert len(rows[rows["entity"] == "Manchester Wind"]) == len(mine)
    # Only the touched component: an axis owned whole obliges the layer to
    # carry that key's extent, not every key's.
    assert set(rows["entity"]) == {"Manchester Wind"}


def test_a_partial_axis_stays_a_patch(staged, root, con):
    """`scenario` *is* partial, so one value may be patched alone.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    # Owned per entity alone: `p_nom` varies over no other axis, so there is no
    # extent along one to restate.
    assert staged.schema.owned_per("p_nom") == frozenset({"entity"})
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_nom"].collect().to_native().to_pandas()
    # `p_nom` varies over nothing, so there is no extent to restate: one row.
    assert len(rows) == 1


# -- add and remove (https://energy-models.github.io/datarecord/design/working-record/#add-remove) --------------------------------------------------


def test_add_then_commit_makes_a_component_exist(staged, root):
    staged.add(
        GEN,
        pd.DataFrame(
            [
                {
                    "entity": "NewSolar",
                    "bus": "Manchester",
                    "role": "attached",
                    "carrier": "solar",
                    "p_nom": 42.0,
                }
            ]
        ),
    )
    members = staged.entity_types[GEN].collect().to_native().to_pandas()
    assert "NewSolar" in set(members["entity"]), "the addition reads back before commit"

    child = staged.commit(NewChild(root))
    assert "NewSolar" in set(child.node_cache.entity_map.df()["entity"])

    static = PyPSA.build(child.record).c[GEN].static
    assert static.loc["NewSolar", "p_nom"] == 42.0
    assert static.loc["NewSolar", "carrier"] == "solar"


def test_add_rejects_a_name_another_type_already_holds(staged):
    """Names are unique across types, enforced at the edit.

    Not left to be discovered: the attribute rows record no type, so two
    components sharing a name would silently share every attribute key.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    with pytest.raises(ValueError, match=r"'Manchester' is already a Bus"):
        staged.add(GEN, pd.DataFrame([{"entity": "Manchester", "carrier": "solar"}]))


def test_add_accepts_a_name_of_its_own_type(staged, root):
    """Re-adding a name of the same type is an edit to that member, not a clash."""
    staged.add(GEN, pd.DataFrame([{"entity": "Manchester Wind", "p_nom": 5.0}]))
    child = staged.commit(NewChild(root))
    assert (
        PyPSA.build(child.record).c[GEN].static.loc["Manchester Wind", "p_nom"] == 5.0
    )


def test_a_name_lookup_stays_a_query_until_it_is_filtered(staged):
    """The type lookup stays lazy, so an edit collects only the names it asks for.

    `set` resolves each named key to its type, so a lookup that pulled every
    name into Python would price one edit at a full resolution.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    names = ["Manchester Wind", "Norway Wind"]
    total = sum(len(staged._resolved_names(ct)) for ct in staged.entity_types)
    assert total > len(names), "fixture too small for the assertion to mean anything"

    assert isinstance(staged._name_types(), nw.LazyFrame)
    assert staged._resolve_types(names) == {
        "Manchester Wind": GEN,
        "Norway Wind": GEN,
    }


def test_resolve_types_rejects_a_name_no_layer_declares(staged):
    """A value keyed to a name with no member row is caught, not dropped.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(KeyError, match="no member row"):
        staged._resolve_types(["Manchester Wind", "Nowhere"])


def test_a_freed_name_may_be_reclaimed_by_another_type(staged, root):
    """`remove` then `add` under another type collapses to the later op.

    The staged entity rows are keyed without `entity_type`, so one name has
    one answer. Partitioning on the type as well would keep both the Generator
    tombstone and the Bus member row, and commit would write a record whose two
    types share a name - the collision `write_record` rejects.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.remove(GEN, ["Manchester Wind"])
    staged.add("Bus", pd.DataFrame([{"entity": "Manchester Wind"}]))

    rows = [
        r
        for r in staged._collapsed_entities("entities").fetchall()
        if r[1] == "Manchester Wind"
    ]
    assert len(rows) == 1
    assert rows[0][0] == "Bus"

    # And it commits: a record with two rows for the name would be rejected.
    child = staged.commit(NewChild(root))
    assert "Manchester Wind" not in PyPSA.build(child.record).c[GEN].static.index


def test_add_routes_a_port_attribute_to_the_connections(staged, root):
    """`bus` keys a connection rather than being a member column.

    Putting it in `dims/entity_type/` would introduce a column the ancestors'
    files lack, which then reads as NULL for their rows - so every existing
    component would lose its bus.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.add(
        GEN,
        pd.DataFrame([{"entity": "NewSolar", "bus": "Manchester", "role": "attached"}]),
    )
    child = staged.commit(NewChild(root))

    buses = PyPSA.build(child.record).c[GEN].static["bus"]
    assert buses["NewSolar"] == "Manchester"
    # The point of the routing: the inherited components keep theirs.
    assert buses["Manchester Wind"] == "Manchester"


def test_add_keeps_an_undeclared_columns_own_type(staged):
    """A member column the schema does not declare stays the type it arrived as.

    The staging table starts with the key columns and gains an attribute column
    as it is first seen, so the type has to come from somewhere. Defaulting to
    `VARCHAR` stored a float as `'1234.5'`, and every consumer of the member
    frame then read text - silently, since a wide member column is never cast
    back the way a staged `value` is.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    assert "capex" not in staged.schema.attributes, "undeclared, which is the case here"
    staged.add(GEN, pd.DataFrame([{"entity": "NewSolar", "capex": 1234.5}]))

    members = staged.entity_types[GEN].collect().to_native().to_pandas()
    capex = members.loc[members["entity"] == "NewSolar", "capex"]
    assert capex.tolist() == [1234.5], "the float survives, rather than becoming text"


def test_add_fills_a_column_an_earlier_add_created(staged):
    """A frame omitting a column another `add` introduced stages it as NULL.

    The staging table gains a column as it is first seen, so a later `add` of
    a different type meets columns its own frame never carried - and an
    earlier one meets those the later `add` introduces. Both are NULL rather
    than an error, which is what the column list being the *table's* rather
    than the frame's has to mean.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    staged.add(GEN, pd.DataFrame([{"entity": "NewSolar", "capex": 1234.5}]))
    # No `capex`, and an `opex` the first frame had no column for.
    staged.add(GEN, pd.DataFrame([{"entity": "NewWind", "opex": 7.0}]))

    members = (
        staged.entity_types[GEN].collect().to_native().to_pandas().set_index("entity")
    )
    assert pd.isna(members.loc["NewWind", "capex"]), "not carried, so NULL"
    assert pd.isna(members.loc["NewSolar", "opex"]), "column created after it was added"
    assert members.loc["NewSolar", "capex"] == 1234.5
    assert members.loc["NewWind", "opex"] == 7.0


def test_remove_tombstones_without_enumerating_attributes(staged, root):
    staged.remove(GEN, ["Norway Gas"])
    members = staged.entity_types[GEN].collect().to_native().to_pandas()
    assert "Norway Gas" not in set(members["entity"]), "the removal reads back at once"

    child = staged.commit(NewChild(root))
    assert "Norway Gas" not in set(child.node_cache.entity_map.df()["entity"])


def test_add_after_remove_leaves_the_component_alive(staged, root):
    """The collapse is per key by `_seq`, so the later `add` wins.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.remove(GEN, ["Norway Gas"])
    staged.add(GEN, pd.DataFrame([{"entity": "Norway Gas", "carrier": "gas"}]))

    child = staged.commit(NewChild(root))
    assert "Norway Gas" in set(child.node_cache.entity_map.df()["entity"])


def test_a_tombstone_drops_that_components_staged_attributes(staged, root):
    """Removing a component discards values staged for it, via the anti-join.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 99.0, entity=["Norway Gas"])
    staged.remove(GEN, ["Norway Gas"])

    child = staged.commit(NewChild(root))
    assert "Norway Gas" not in _static(child, "p_nom")


# -- connect and disconnect (https://energy-models.github.io/datarecord/design/working-record/#add-remove, https://energy-models.github.io/datarecord/design/record/#connections) --------------------------------------


def test_add_group_stages_a_new_connection(staged, root):
    """A connection is a row keyed by `(name, bus)`, not a positional column.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.add_group(
        "connection",
        pd.DataFrame(
            [{"entity": "Manchester Wind", "bus": "Norway", "role": "attached"}]
        ),
    )
    staged_rows = staged.groups["connection"].collect().to_native().to_pandas()
    assert "Norway" in set(
        staged_rows[staged_rows["entity"] == "Manchester Wind"]["bus"]
    ), "the new connection reads back before commit"

    child = staged.commit(NewChild(root))
    rows = child.node_cache.group_frame("connection").df()
    got = set(rows[rows["entity"] == "Manchester Wind"]["bus"])
    assert "Norway" in got


def test_remove_group_stages_a_tombstone(staged, root):
    """One `deleted` row per `(entity, bus)`, the group's own key.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.remove_group("connection", [("Norwich Converter", "Norwich")])
    before = staged.groups["connection"].collect().to_native().to_pandas()
    ports = set(before[before["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in ports, "the removal reads back before commit"
    assert "Norwich DC" in ports, "deletion is per connection, not per component"

    child = staged.commit(NewChild(root))
    rows = child.node_cache.group_frame("connection").df()
    left = set(rows[rows["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in left
    # The component's other port survives: deletion is per connection, not per
    # component (https://energy-models.github.io/datarecord/design/record/#connections).
    assert "Norwich DC" in left


def test_add_group_needs_every_coordinate(staged):
    with pytest.raises(ValueError, match="'bus'"):
        staged.add_group("connection", pd.DataFrame([{"entity": "Manchester Wind"}]))


def test_every_declared_group_reads_its_staged_rows(con, base_uri, ac_dc):
    """A second group is not silently dropped: the reads are keyed by group.

    `connection` is the group every fixture has, so a read path naming it rather
    than iterating the declared ones would pass everywhere except here - a
    record declaring a `corridor` would read nothing while holding rows to
    commit.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_schema(
        schema(
            groups={
                "connection": {"entity": "entity", "bus": "bus"},
                "corridor": {"from": "entity", "to": "entity"},
            }
        )
    )
    staged = WorkingRecord(revision.record, con)

    staged.add_group(
        "connection", pd.DataFrame([{"entity": "Manchester Wind", "bus": "Norway"}])
    )
    staged.add_group(
        "corridor", pd.DataFrame([{"from": "Manchester Wind", "to": "Norway"}])
    )

    connections = staged.groups["connection"].collect().to_native().to_pandas()
    assert "Norway" in set(
        connections[connections["entity"] == "Manchester Wind"]["bus"]
    )

    corridors = staged.groups["corridor"].collect().to_native().to_pandas()
    assert list(zip(corridors["from"], corridors["to"], strict=True)) == [
        ("Manchester Wind", "Norway")
    ], "the second group reads its own rows, not the first's"


# -- rollback (https://energy-models.github.io/datarecord/design/working-record/#committing) --------------------------------------------------------


def test_rollback_discards_everything_staged(staged, root):
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    staged.remove(GEN, ["Norway Gas"])
    staged.rollback()

    # The record reads the base rows again, edit and tombstone alike.
    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert 0.42 not in set(rows["value"])
    members = staged.entity_types[GEN].collect().to_native().to_pandas()
    assert "Norway Gas" in set(members["entity"]), "the tombstone went with the rest"


def test_commit_clears_the_staging_area(staged, root):
    """A second commit writes nothing: the edits left with the first.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    staged.commit(NewChild(root))

    again = staged.commit(NewChild(root))
    layer = DirectoryRecord(layer_dir(again.id), con=staged.con)
    assert "p_nom" not in layer.attributes


# -- commit targets (https://energy-models.github.io/datarecord/design/working-record/#committing) --------------------------------------------------


def test_a_child_layer_holds_only_the_edits(staged, root, con):
    """A patch layer is the edits alone; the fold resolves the rest.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_nom"].collect().to_native().to_pandas()
    assert list(rows["entity"]) == ["Manchester Wind"]
    # Yet the resolved record reads every generator's value.
    assert len(_static(child, "p_nom")) > 1


def test_a_completed_axis_carries_the_touched_key_and_no_other(staged, root, con):
    """Owning `snapshot` whole completes the edited series, not every series.

    `p_max_pu` varies over a non-partial `snapshot`, so touching one of
    Manchester Wind's snapshots makes the layer the owner of that generator's
    whole series and it must carry the untouched snapshots too. The scope is the
    key the edit named: a second generator the edit never mentioned stays in the
    parent, and a layer carrying it would claim an extent it was never given.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)
    assert len(mine) > 1, "a one-snapshot series would not distinguish the two scopes"

    staged.set("p_max_pu", one, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert set(rows["entity"]) == {"Manchester Wind"}, (
        "only the touched key's extent is carried"
    )
    written = rows.sort_values("snapshot")
    assert written["value"].tolist() == [0.123, *mine.iloc[1:]["value"].tolist()], (
        "the edit, then the rest of the series it now owns"
    )


def test_new_child_defaults_to_the_node_the_record_was_built_over(staged, root):
    """`NewChild()` branches from the base, which is what a caller means.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = staged.commit(NewChild())

    assert child.parent == root.id
    assert _static(child, "p_nom")["Manchester Wind"] == 150.0


def test_the_edits_land_in_the_child_not_the_node_branched_from(staged, root, con):
    """Layers are write-once, so the parent still reads its own values.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    before = _static(root, "p_nom")["Manchester Wind"]
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = staged.commit(NewChild())

    assert _static(child, "p_nom")["Manchester Wind"] == 150.0
    assert _static(root, "p_nom")["Manchester Wind"] == before


def test_new_child_without_a_layered_base_says_what_to_pass(con, base_uri, tmp_path):
    """A directory is no node in a tree, so there is nothing to branch from.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    revision = Revision.create(con)
    write_schema(read_schema(con), base_uri)
    over_a_directory = WorkingRecord(DirectoryRecord(layer_dir(revision.id), con), con)

    with pytest.raises(ValueError, match="needs a revision to branch from"):
        over_a_directory.commit(NewChild())


def test_a_directory_target_writes_a_flattened_record(staged, root, con, tmp_path):
    """No parent to resolve against, so the whole record is written.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    out = str(tmp_path / "flat")
    assert staged.commit(Directory(out)) is None

    record = DirectoryRecord(out, con)
    rows = record.attributes["p_nom"].collect().to_native().to_pandas()
    got = dict(zip(rows["entity"], rows["value"], strict=True))
    assert got["Manchester Wind"] == 150.0
    # Flattened: every component is present, not left to a parent to supply.
    members = record.entity_types[GEN].collect().to_native().to_pandas()
    assert len(members) == 6


def test_a_committed_child_builds_a_network(staged, root):
    """The whole point: an edited record is still a buildable model.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    assert (
        PyPSA.build(child.record).c[GEN].static.loc["Manchester Wind", "p_nom"] == 150.0
    )


# -- the `Expr` value form's raise rule (https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one) -------------------------------


def test_an_expression_over_a_named_target_with_no_rows_raises(staged):
    """A named target that resolves to nothing is a failed change, not a no-op.

    The caller asked for these rows to take a new value and there is nothing to
    derive one from, so it fails loudly rather than staging zero rows.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    with pytest.raises(KeyError, match="no current value to derive from"):
        staged.set(
            "p_max_pu",
            nw.col("value") * 2,
            entity=["Manchester Wind"],
            snapshot="1999-01-01",
        )


def test_an_unscoped_expression_over_an_absent_attribute_stages_nothing(staged):
    """`entity=None` and no scope means "whatever resolves", so empty is an answer."""
    absent = next(
        a
        for a in sorted(staged.schema.attributes_for(GEN))
        if a not in staged.attributes
    )
    staged.set(absent, nw.col("value") * 2)
    assert absent not in staged.attributes, "nothing resolved, so nothing was staged"


# -- results through `kind="outputs"` (https://energy-models.github.io/datarecord/design/working-record/#set, https://energy-models.github.io/datarecord/design/read-path/#outputs) ---------------------------


def test_results_stage_and_read_back_without_committing(staged):
    """A tool can attach what it solved and the record reads it.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    assert list(staged.outputs) == []
    staged.set("p", 42.0, entity=["Manchester Wind"], kind="outputs")

    rows = staged.outputs["p"].collect().to_native().to_pandas()
    assert dict(zip(rows["entity"], rows["value"], strict=True)) == {
        "Manchester Wind": 42.0
    }
    # Staged as a result, so it is not an input.
    assert "p" not in staged.attributes


def test_results_survive_a_commit_into_the_new_layer(staged, root, con):
    """Staged results land in the child's `outputs/`, alongside its inputs.

    Notes
    -----
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    staged.set("p", 42.0, entity=["Manchester Wind"], kind="outputs")
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    assert "p" in layer.outputs
    rows = layer.outputs["p"].collect().to_native().to_pandas()
    assert rows["value"].tolist() == [42.0]
    # And the inputs went where inputs go.
    assert "p_nom" in layer.attributes


def test_results_accept_a_component_type_the_record_never_declared(staged):
    """A solve may derive a component the record has no member row for.

    PyPSA's `SubNetwork` is the real case: it exists only after a solve, so
    requiring a declared member row - which an *input* value must have -
    would refuse a legitimate result.

    Notes
    -----
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    staged.set("sub_network", "AC", entity=["1"], kind="outputs")
    rows = staged.outputs["sub_network"].collect().to_native().to_pandas()
    assert rows["entity"].tolist() == ["1"]

    # The same name as an input is still rejected: membership governs inputs.
    with pytest.raises(KeyError, match="member row"):
        staged.set("p_nom", 1.0, entity=["NoSuchGenerator"])


def test_an_undeclared_result_is_rejected(staged):
    """A result is declared like an input, so a typo is caught where it is typed.

    The membership rule stays relaxed either way - it is the *attribute* that
    must be declared, not the component it names.

    Notes
    -----
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(KeyError, match="no result 'p_nom_optt'"):
        staged.set("p_nom_optt", 1.0, entity=["Manchester Wind"], kind="outputs")


def test_a_result_may_not_vary_over_a_dim_it_does_not_declare(staged):
    """`results` carries coordinates like `attributes`, so the scope is checked.

    Notes
    -----
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    """
    assert "period" not in staged.schema.results["p_nom_opt"].dims
    with pytest.raises(ValueError, match="does not vary over"):
        staged.set(
            "p_nom_opt", 1.0, entity=["Manchester Wind"], kind="outputs", period=2030
        )


def test_a_results_value_keeps_its_declared_type(staged):
    """A result round-trips at the dtype `results` declares, not a guessed one.

    Staged values are held as text, since one staging table serves every
    attribute, and reading one back casts to the declared dtype. Both halves
    have to hold at once: PyPSA's `sub_network` is a string-valued output, and
    casting it as a number would `TRY_CAST` it to NULL and lose it with nothing
    raised, while `p` must come back a number rather than the text it was held
    as.

    Notes
    -----
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    """
    assert "sub_network" in staged.schema.results, "declared as a result, not an input"
    assert "sub_network" not in staged.schema.attributes
    staged.set("sub_network", "0", entity=["Manchester Wind"], kind="outputs")

    rows = staged.outputs["sub_network"].collect().to_native().to_pandas()
    assert rows["value"].tolist() == ["0"], "a string result survives the round trip"

    # The other half: dropping the cast altogether would answer '42.0' here,
    # a number read back as the text the staging column holds it as.
    staged.set("p", 42.0, entity=["Manchester Wind"], kind="outputs")
    numeric = staged.outputs["p"].collect().to_native().to_pandas()
    assert numeric["value"].tolist() == [42.0], "a numeric result comes back a number"


def test_a_multi_type_results_frame_stages_by_name_alone(staged, root, con):
    """One frame spanning types is one call, keyed by name alone.

    `Tool.results` hands over one frame per attribute carrying every type's rows; with names unique there is no type to stamp, so the frame needs no
    `entity_type` and nothing can be silently relabelled.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    frame = pd.DataFrame(
        [
            {"entity": "Manchester Wind", "value": 1.0},
            {"entity": "0", "value": 2.0},
        ]
    )
    staged.set("p", frame, kind="outputs")

    rows = staged.outputs["p"].collect().to_native().to_pandas()
    assert dict(zip(rows["entity"], rows["value"], strict=True)) == {
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
    assert set(got["entity"]) == {"Manchester Wind", "0"}
    assert "entity_type" not in got.columns


def test_a_frame_carrying_component_type_is_rejected(staged):
    """The column says the writer thinks the type keys the row; it does not.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    frame = pd.DataFrame(
        [{"entity_type": GEN, "entity": "Manchester Wind", "value": 1.0}]
    )
    with pytest.raises(ValueError, match="entity_type"):
        staged.set("p_max_pu", frame)


def test_a_scalar_derives_the_type_from_the_name(staged):
    """No type keyword: the name determines it, so a bare `set` is enough.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    assert set(rows[rows["entity"] == "Manchester Wind"]["value"]) == {150.0}


def test_one_call_spans_component_types(staged):
    """Names decide the type, so one edit may cross types.

    `p_nom` is declared for both `Generator` and `Link`, and each name is
    validated against its own type's spec - one call, two types, no keyword.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged.set("p_nom", {"Manchester Wind": 150.0, "DC link": 80.0})
    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    got = dict(zip(rows["entity"], rows["value"], strict=True))
    assert got["Manchester Wind"] == 150.0
    assert got["DC link"] == 80.0


def test_an_attribute_the_names_type_does_not_declare_is_rejected(staged):
    """The spec checked is the name's *own* type's, and the error names the name.

    `p_max_pu` is a Generator attribute and `London Load` is a Load, so the
    failure is reported against the name that caused it rather than against a
    type the caller never mentioned.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(KeyError, match="London Load"):
        staged.set("p_max_pu", {"Manchester Wind": 0.5, "London Load": 0.5})
