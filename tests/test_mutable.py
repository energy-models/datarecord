# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`WorkingRecord`: staging, the edit operations, writing the edits.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Workspace
from datarecord.layered.record import Record
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.layered.sources import ParquetLayer
from datarecord.mutable import WorkingRecord, normalise_value
from datarecord.record import RecordLike
from datarecord.schema import AttributeSpec, Dimension, PropertySpec, Schema, Trait
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import empty_root, export_network, schema, unpacked

GEN = "Generator"


@pytest.fixture
def root(store, ac_dc):
    """A materialised record to branch edits from."""
    revision = export_network(ac_dc, store)
    revision.materialise()
    return revision


@pytest.fixture
def staged(root, store):
    return WorkingRecord(root.record)


@pytest.fixture
def written_directory(root, tmp_path):
    """A standalone directory, as a URI a caller could pass to `Workspace.at`."""
    out = str(tmp_path / "standalone")
    root.store.workspace.write(root.record, out)
    return out


def _static(revision, attribute, ctype=GEN):
    """One attribute as the built network sees it, per component name.

    Through the build rather than `relation()`: a non-varying attribute like
    `p_nom` lives in `dims/entity.parquet`, so `inputs/` alone would not
    show what the record resolves to.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return PyPSA.build(revision.record).c[ctype].static[attribute].to_dict()


def _layer_rows(revision, attribute, kind="inputs"):
    """One committed layer's own `<kind>/<attribute>.parquet`, as pandas.

    The single-layer view, read through the `LayerSource` for that layer rather
    than the folding resolver: "what did this patch write" is a question about
    one layer's file, not the resolved record, so `Store.layer` - which folds a
    source through the whole-tree machinery - is the wrong lens for it.
    """
    rel = ParquetLayer(
        revision.id, read_schema(revision.store), revision.store
    ).attribute(attribute, kind)
    return rel.to_df() if rel is not None else pd.DataFrame()


def _layer_axis(revision, dim):
    """One committed layer's own `dims/<dim>.parquet`, as pandas - `_layer_rows` for an axis."""
    rel = ParquetLayer(revision.id, read_schema(revision.store), revision.store).axis(
        dim
    )
    return rel.to_df() if rel is not None else pd.DataFrame()


def _unpacked(frame, group="connection"):
    """A group attribute's rows with the group's struct spread flat, as pandas."""
    native = frame.collect().to_native().to_pandas()
    fields = pd.DataFrame(native.pop(group).tolist(), index=native.index)
    return pd.concat([fields, native], axis=1)


# -- the protocol (https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit) ----------------------------------------------------


def test_a_working_record_overrides_no_read_member():
    """Every member it defines beyond `Record`'s is an edit, `edits`, or private.

    The property the whole design rests on: a staged edit is read by the same
    fold that reads a committed layer, so a *read* member `WorkingRecord`
    redefines is a place where staging stopped being just another layer. No
    behavioural test would name that, because both paths would still answer -
    they would just be two paths again.

    `outputs` is the one allowed exception, results not overlaying, and it is
    listed so that adding a second one requires editing this line.
    """
    edits = {
        "set",
        "add",
        "remove",
        "add_group",
        "remove_group",
        "rollback",
        "edits",
    }
    allowed_read_override = {"outputs"}

    inherited = {n for n in vars(Record) if not n.startswith("_")}
    defined = {n for n in vars(WorkingRecord) if not n.startswith("_")}
    assert inherited & defined <= allowed_read_override, (
        "a read member redefined here means staging is no longer just a layer"
    )
    assert defined - inherited == edits, (
        "a public member that is neither an edit nor an inherited read"
    )


def test_a_mutable_record_reads_as_a_record(staged):
    """Editable *and* readable: the pending edits are a layer, so reads compose.

    The load-bearing half of `WorkingRecord`: it satisfies `Record`, so what
    it reads is the data with its pending edits applied and it can be handed
    to anything that only knows how to read.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
    """
    assert isinstance(staged, RecordLike)


# -- value forms (https://energy-models.github.io/datarecord/design/working-record/#set) -----------------------------------------------------


def test_a_scalar_has_no_keys():
    assert normalise_value(150.0, ["wind1", "wind2"]) == (None, [150.0])


def test_a_sequence_is_positional():
    keys, values = normalise_value([150.0, 80.0], ["wind1", "wind2"])
    assert keys is not None
    assert dict(zip(keys, values, strict=True)) == {"wind1": 150.0, "wind2": 80.0}


def test_a_mapping_supplies_its_own_keys():
    keys, values = normalise_value({"wind1": 150.0, "wind2": 80.0})
    assert keys is not None
    assert dict(zip(keys, values, strict=True)) == {"wind1": 150.0, "wind2": 80.0}


def test_a_series_supplies_its_index():
    series = pd.Series({"2030-01-01": 0.4, "2030-01-02": 0.6})
    assert normalise_value(series) == (["2030-01-01", "2030-01-02"], [0.4, 0.6])


def test_a_sequence_of_the_wrong_length_is_rejected():
    with pytest.raises(ValueError, match="2 labels"):
        normalise_value([1.0, 2.0, 3.0], ["wind1", "wind2"])


def test_a_sequence_needs_labels_to_align_to():
    with pytest.raises(ValueError, match="list keyword"):
        normalise_value([1.0, 2.0])


def test_indexed_by_names_the_axis_a_series_index_holds(staged):
    """Said outright, since nothing about the labels themselves could say it."""
    series = pd.Series({pd.Timestamp("2015-01-01"): 0.4})
    staged.set("p_max_pu", series, entity=["Manchester Wind"], indexed_by="snapshot")
    assert "p_max_pu" in staged.attributes


def test_an_unnamed_series_index_over_several_open_coordinates_is_refused(staged):
    """Not inferred from the labels, so an index saying nothing must be told.

    `entity=` fixes one coordinate, but `p_max_pu` has more than one left open,
    so nothing says which the index holds.
    """
    series = pd.Series({"2030-01-01": 0.4})
    with pytest.raises(ValueError, match="indexed_by="):
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

    assert _entity_axis(staged).loc["Manchester Wind", "p_nom"] == 150.0
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


def test_two_lazy_reads_stay_bound_to_their_own_relations(staged, store):
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
    views = {
        v
        for (v,) in store.workspace.con.sql(
            "SELECT view_name FROM duckdb_views()"
        ).fetchall()
    }
    assert not {v for v in views if v.startswith("_")}


def test_last_write_wins_within_the_staging_area(staged, root):
    """Two edits to one key collapse to the later one, by `_seq`.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.set("p_nom", 100.0, entity=["Manchester Wind"])
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    # Read back before commit: the collapse is what the read applies too, so
    # the second edit wins without the rows having been merged when staged.
    axis = _entity_axis(staged)
    assert axis.loc[["Manchester Wind"], "p_nom"].tolist() == [150.0], (
        "one row survives the collapse, the later"
    )

    child = root.child(staged.edits)
    assert _static(child, "p_nom")["Manchester Wind"] == 150.0


def test_set_over_several_names(staged, root):
    staged.set("p_nom", 150.0, entity=["Manchester Wind", "Norway Wind"])
    child = root.child(staged.edits)

    got = _static(child, "p_nom")
    assert got["Manchester Wind"] == got["Norway Wind"] == 150.0


def test_set_rejects_an_unknown_name(staged):
    """A value for a name no layer declares would resolve to nothing.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    with pytest.raises(KeyError, match="Nope"):
        staged.set("p_max_pu", 1.0, entity=["Nope"])


def test_set_accepts_a_name_staged_by_add(staged, root):
    """`add` makes the name exist, so a value for it is no longer unknown.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    staged.add("entity", ["NewSolar"], entity_type=GEN)
    staged.set("p_nom", 7.0, entity=["NewSolar"])

    child = root.child(staged.edits)
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

    staged.set("p_max_pu", one)

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

    child = root.child(staged.edits)
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
    before = staged.flags()["marginal_cost"]
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
    )

    after = staged.flags()["marginal_cost"]
    assert "snapshot" in after.varies
    assert after.broadcast == before.broadcast


def test_a_staged_edit_is_read_back_and_then_re_read(staged, ac_dc):
    """Set, read, set again, read again: the second read must see the second edit.

    That is the whole content of "the staged step is not cached", and it fails
    loudly if anyone later materialises past the frozen prefix - a
    `cached_property` over the tail, or a `.create()` that does not stop where
    `frozen` does. The first read is what arms it: it builds the fold, so a
    cache introduced anywhere in it would be populated before the second `set`.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    """

    def p_nom(name):
        return _entity_axis(staged).loc[[name], "p_nom"].tolist()

    staged.set("p_nom", 11.0, entity=["Manchester Wind"])
    assert p_nom("Manchester Wind") == [11.0]

    staged.set("p_nom", 22.0, entity=["Manchester Wind"])
    assert p_nom("Manchester Wind") == [22.0], "the read re-folds the staged layer"

    # A key the second edit never named, to pin that re-folding is not
    # re-resolving from scratch and dropping what the first edit staged.
    staged.set("p_nom", 33.0, entity=["Norway Wind"])
    assert p_nom("Manchester Wind") == [22.0]
    assert p_nom("Norway Wind") == [33.0]


def test_flags_do_not_depend_on_the_maps_grouping_grain(staged, ac_dc):
    """The fold flags per key; `flags` unions across a type. Both must agree.

    The staged and parquet paths share one aggregate exactly because the
    grouping grain is invisible to the answer: the map computes `varies` and
    `broadcast` per owner-map key, and `attributes_of` `bool_or`-unions them
    over a type's members. So a type whose components are staged at different
    grains - one per-snapshot, one broadcast - must report both sets, the same
    answer either grouping would give.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
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
    )
    staged.set("marginal_cost", 3.0, entity=["Norway Wind"])

    flags = staged.flags()["marginal_cost"]
    assert "snapshot" in flags.varies, "one member's rows name the snapshot"
    assert "snapshot" in flags.broadcast, "another's leave it NULL"


# -- value dtypes (https://energy-models.github.io/datarecord/design/record/#flags, https://energy-models.github.io/datarecord/design/schema/#attributespec) -----------------------------------------------


def test_a_non_float_attribute_stages_and_commits(root, store):
    """`value` carries the attribute's declared dtype, not always `DOUBLE`.

    One staging table holds every attribute's values, so it stages `value` as
    text and casts to the declared dtype where the attribute is known - which
    is the point at which `inputs/<attr>.parquet` is per-attribute.

    The schema is amended before the `WorkingRecord` is built: a record carries
    the schema its base was resolved under, so a widening has to be in force
    when the record is constructed, not slipped in before a later commit.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    amended = read_schema(store)
    amended.attributes["carrier"] = AttributeSpec(
        dtype=nw.String(), dims={"entity", "scenario"}
    )
    write_schema(amended, store)

    staged = WorkingRecord(root.record)
    staged.set("carrier", "solar", entity=["Manchester Wind"])
    rows = staged.attributes["carrier"].collect().to_native().to_pandas()
    assert rows[rows["entity"] == "Manchester Wind"]["value"].tolist() == ["solar"]

    child = root.child(staged.edits)
    got = child.record.attributes["carrier"].collect().to_native().to_pandas()
    assert got[got["entity"] == "Manchester Wind"]["value"].tolist() == ["solar"]


def test_a_float_attribute_stays_numeric(staged):
    """The cast is per attribute, so a `DOUBLE` one is not turned into text."""
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    column = _entity_axis(staged)["p_nom"]
    assert column.dtype.kind == "f"
    assert 150.0 in set(column)


# -- ownership granularity (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override, https://energy-models.github.io/datarecord/design/working-record/#edits) -------------------------------------


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
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    assert "snapshot" not in (staged.schema.partial or frozenset())
    # Owned per entity, since a layer patches one component without restating
    # the rest - but not per snapshot, which is the axis this test is about.
    assert "snapshot" not in staged.schema.owned_per("p_max_pu")

    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(mine) > 1
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one)
    child = root.child(staged.edits)

    got = child.record.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = got[got["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(got) == len(mine)
    # The edit applied, and every other hour kept the parent's value.
    assert got.iloc[0]["value"] == 0.123
    assert got.iloc[1:]["value"].tolist() == mine.iloc[1:]["value"].tolist()


def test_the_restated_series_is_in_the_layer_itself(staged, root, store):
    """The layer carries the whole extent, not a parent lookup at read time.

    A patch layer holds only edits - except along an axis owned whole,
    where the completed series must be in the layer, since that is what makes
    this layer its owner.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"]
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one)
    child = root.child(staged.edits)

    rows = _layer_rows(child, "p_max_pu")
    assert len(rows[rows["entity"] == "Manchester Wind"]) == len(mine)
    # Only the touched component: an axis owned whole obliges the layer to
    # carry that key's extent, not every key's.
    assert set(rows["entity"]) == {"Manchester Wind"}


def test_two_edits_on_one_whole_owned_series_both_land(staged, root, store):
    """A second `set` replaces its own fill without disturbing the first edit's.

    The case `staging-without-seq` leans on hardest: a `set` on a whole-owned
    axis stages the touched value plus the untouched coordinates as fills, and a
    second `set` at another coordinate must replace only the fill on *its* key -
    the first edit's value and its fills have to survive. Idempotence of
    `_complete_owned_whole` and replace-by-coordinate are what make it hold with
    no ordering column to rank a fill below an edit.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(mine) > 2, "the series needs an untouched middle to keep"

    first = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.111)
    second = mine.iloc[[1]][["entity", "snapshot"]].assign(value=0.222)
    staged.set("p_max_pu", first)
    staged.set("p_max_pu", second)

    child = root.child(staged.edits)
    rows = _layer_rows(child, "p_max_pu")
    got = rows[rows["entity"] == "Manchester Wind"].sort_values("snapshot")

    assert len(got) == len(mine), "the whole extent is carried, once per snapshot"
    assert got.iloc[0]["value"] == 0.111, "the first edit survives the second"
    assert got.iloc[1]["value"] == 0.222, "the second edit replaced its own fill"
    assert got.iloc[2:]["value"].tolist() == mine.iloc[2:]["value"].tolist(), (
        "every untouched snapshot keeps the base value"
    )


def test_a_partial_axis_stays_a_patch(staged, root, store):
    """`scenario` *is* partial, so one value may be patched alone.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = root.child(staged.edits)

    rows = _layer_axis(child, "entity")
    assert list(rows["entity"]) == ["Manchester Wind"], (
        "`p_nom` is an entity column, so the patch is one axis row"
    )


# -- add and remove (https://energy-models.github.io/datarecord/design/working-record/#add-remove) --------------------------------------------------


def _entity_axis(record):
    """The record's resolved `dims["entity"]`, indexed by label, as pandas."""
    frame = record.dims["entity"].collect().to_native().to_pandas()
    return frame.set_index("entity")


def test_add_then_commit_makes_a_label_exist(staged, root):
    staged.add("entity", ["NewSolar"], entity_type=GEN)
    assert "NewSolar" in _entity_axis(staged).index, "it reads back before commit"

    child = root.child(staged.edits)
    assert _entity_axis(child.record).loc["NewSolar", "entity_type"] == GEN


def test_add_rejects_a_label_the_discriminator_does_not_list(staged):
    """The discriminator's `Enum` is the vocabulary, checked at the edit.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(ValueError, match="Nope"):
        staged.add("entity", ["NewSolar"], entity_type="Nope")


def test_the_discriminator_is_generic_over_dim_and_property(store):
    """Any dim may carry the discriminator, under any property name.

    A `unit` axis classified by `kind`: `add` checks `kind` against its `Enum`,
    and a `set` on a label checks the label exists and that its kind carries
    the attribute.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
    """
    write_schema(
        Schema(
            dimensions={
                "unit": Dimension(dtype=nw.String()),
                "hour": Dimension(dtype=nw.Int64()),
            },
            properties={
                "unit": {
                    "kind": PropertySpec(
                        dtype=nw.Enum(["plant", "node"]), discriminator=True
                    )
                }
            },
            attributes={
                "output": AttributeSpec(dtype=nw.Float64(), dims={"unit", "hour"})
            },
            traits={
                "plant": Trait(
                    attributes=frozenset({"output"}), on={"kind": frozenset({"plant"})}
                )
            },
            partial=frozenset({"unit"}),
        ),
        store,
    )
    staged = WorkingRecord(empty_root(store).record)

    with pytest.raises(ValueError, match="'kind' values \\['mill'\\]"):
        staged.add("unit", ["m"], kind="mill")
    staged.add("unit", ["p", "n"], kind=["plant", "node"])

    staged.set("output", 1.0, unit=["p"], hour=[0])
    with pytest.raises(KeyError, match="'n' is a node"):
        staged.set("output", 1.0, unit=["n"], hour=[0])
    with pytest.raises(KeyError, match="no 'unit' label \\['x'\\]"):
        staged.set("output", 1.0, unit=["x"], hour=[0])


def test_re_adding_a_label_replaces_its_row(staged, root):
    """One axis row per label: a later `add` is the row, whatever kind it names.

    Uniqueness needs no check of its own - the axis is keyed by the label, so
    a `remove` then an `add` under another kind is one row, the later.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.remove("entity", ["Manchester Wind"])
    staged.add("entity", ["Manchester Wind"], entity_type="Bus")

    rows = staged.edits.axis("entity").df()
    rows = rows[rows["entity"] == "Manchester Wind"]
    assert len(rows) == 1, "one label, one row"
    assert list(rows["entity_type"]) == ["Bus"], "the later op wins"

    child = root.child(staged.edits)
    assert _entity_axis(child.record).loc["Manchester Wind", "entity_type"] == "Bus"


def test_add_rejects_a_column_the_schema_does_not_declare(staged):
    """An axis column no declaration accounts for is an error, not a new column.

    A staging table is shaped like the file it becomes, from the schema, so an
    undeclared column has no dtype to be given it. A tool that grows a column
    declares it first.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    assert "capex" not in staged.schema.attributes, "undeclared, which is the case here"
    with pytest.raises(ValueError, match="capex"):
        staged.add("entity", ["NewSolar"], capex=1234.5)


def test_add_fills_a_declared_column_another_add_omitted(staged):
    """An `add` omitting a declared column stages it as NULL rather than failing.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    staged.add("entity", ["NewSolar"], entity_type=GEN)
    staged.add("entity", ["NewWind"])

    rows = staged.edits.axis("entity").df().set_index("entity")
    assert pd.isna(rows.loc["NewWind", "entity_type"]), "not carried, so NULL"
    assert rows.loc["NewSolar", "entity_type"] == GEN, "unaffected by the later add"


def test_remove_tombstones_without_enumerating_attributes(staged, root):
    staged.remove("entity", ["Norway Gas"])
    assert "Norway Gas" not in _entity_axis(staged).index, "it reads back at once"

    child = root.child(staged.edits)
    assert "Norway Gas" not in set(child.resolver.axis("entity").df()["entity"])


def test_add_after_remove_leaves_the_label_alive(staged, root):
    """`add` replaces the staged tombstone rather than sitting beside it.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.remove("entity", ["Norway Gas"])
    staged.add("entity", ["Norway Gas"], entity_type=GEN)

    child = root.child(staged.edits)
    assert "Norway Gas" in set(child.resolver.axis("entity").df()["entity"])


def test_a_tombstone_drops_that_labels_staged_attributes(staged, root):
    """Removing a label discards values staged for it, via the anti-join.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.set("p_nom", 99.0, entity=["Norway Gas"])
    staged.remove("entity", ["Norway Gas"])

    child = root.child(staged.edits)
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
            [
                {
                    "connection": {"entity": "Manchester Wind", "bus": "Norway"},
                    "role": "attached",
                }
            ]
        ),
    )
    staged_rows = unpacked(staged.groups["connection"])
    assert "Norway" in set(
        staged_rows[staged_rows["entity"] == "Manchester Wind"]["bus"]
    ), "the new connection reads back before commit"

    child = root.child(staged.edits)
    rows = unpacked(child.resolver.group_frame("connection"))
    got = set(rows[rows["entity"] == "Manchester Wind"]["bus"])
    assert "Norway" in got


def test_remove_group_stages_a_tombstone(staged, root):
    """One `deleted` row per `(entity, bus)`, the group's own key.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.remove_group("connection", [("Norwich Converter", "Norwich")])
    before = unpacked(staged.groups["connection"])
    ports = set(before[before["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in ports, "the removal reads back before commit"
    assert "Norwich DC" in ports, "deletion is per connection, not per component"

    child = root.child(staged.edits)
    rows = unpacked(child.resolver.group_frame("connection"))
    left = set(rows[rows["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in left
    # The component's other port survives: deletion is per connection, not per
    # component (https://energy-models.github.io/datarecord/design/record/#connections).
    assert "Norwich DC" in left


def _efficiency(record, entity="Manchester Wind"):
    """`efficiency` per bus for one entity, as the record resolves it."""
    rows = _unpacked(record.attributes["efficiency"])
    return rows[rows["entity"] == entity].groupby("bus")["value"].unique().to_dict()


def test_a_bus_keyword_addresses_one_connection(staged, root):
    """`bus=` names one connection, which the stage packs into its struct.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    staged.set("efficiency", 0.5, entity=["Manchester Wind"], bus="Manchester")

    own = staged.edits.attribute("efficiency")
    assert "bus" not in own.columns, "the row carries the connection, not its bus"
    assert own.project("connection").distinct().fetchall() == [
        ({"entity": "Manchester Wind", "bus": "Manchester"},)
    ]
    assert list(_efficiency(staged)["Manchester"]) == [0.5]

    child = root.child(staged.edits)
    assert list(_efficiency(child.record)["Manchester"]) == [0.5]


def test_an_omitted_group_coordinate_is_rejected(staged):
    """No `bus=` would key a row by `{entity, NULL}`, which no connection matches.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    with pytest.raises(ValueError, match=r"\['bus'\] was not given"):
        staged.set("efficiency", 0.5, entity=["Manchester Wind"])


def test_a_frame_carries_a_group_as_its_struct(staged):
    """A packed struct passes through, one key replacing the last.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    for value in (0.5, 0.25):
        staged.set(
            "efficiency",
            staged.con.sql(
                "SELECT {'entity': 'Manchester Wind', 'bus': 'Manchester'} "
                f"AS connection, {value} AS value"
            ),
        )
    assert list(_efficiency(staged)["Manchester"]) == [0.25], "one key, replaced"


@pytest.mark.parametrize(
    "select",
    [
        "'Manchester Wind' AS entity, 'Manchester' AS bus, 0.5 AS value",
        "{'entity': 'Manchester Wind', 'bus': 'Manchester'} AS connection, "
        "'Manchester' AS bus, 0.5 AS value",
        "{'entity': 'Manchester Wind'} AS connection, 0.5 AS value",
        "'Manchester' AS connection, 0.5 AS value",
        "{'entity': 'Manchester Wind', 'bus': NULL::VARCHAR} AS connection, "
        "0.5 AS value",
    ],
    ids=["flat", "both", "partial-struct", "not-a-struct", "partly-null"],
)
def test_a_frame_carrying_a_group_but_as_its_struct_is_rejected(staged, select):
    """Flat coordinates, beside the struct or not, or a wrong struct, are refused.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    with pytest.raises(ValueError, match="'connection'"):
        staged.set("efficiency", staged.con.sql(f"SELECT {select}"))


def test_an_expression_scoped_by_bus_derives_one_connection(staged):
    """`bus=` scopes a derived value through the struct field.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    staged.set("efficiency", 0.5, entity=["Manchester Gas"], bus="Manchester")
    staged.set(
        "efficiency", nw.col("value") * 2, entity=["Manchester Gas"], bus="Manchester"
    )
    assert list(_efficiency(staged, "Manchester Gas")["Manchester"]) == [1.0]


def test_a_result_takes_a_bus_keyword(staged):
    """A result over `connection` is addressed by `bus=` like an input.

    Notes
    -----
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    """
    staged.set("p", 1.0, entity=["Manchester Wind"], bus="Manchester", kind="outputs")
    rows = _unpacked(staged.outputs["p"])
    assert list(zip(rows["entity"], rows["bus"], strict=True)) == [
        ("Manchester Wind", "Manchester")
    ]


def test_remove_drops_the_values_on_its_connections(staged, root):
    """`remove` tombstones the entity's connections, and their values go with them.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    - [deletion cascades through tombstones](https://energy-models.github.io/datarecord/design/proposals/deletion-cascades-through-tombstones/)
    """
    assert _efficiency(staged), "the base carries a value to lose"
    staged.set("efficiency", 0.5, entity=["Manchester Wind"], bus="Manchester")
    staged.remove("entity", ["Manchester Wind"])

    assert _efficiency(staged) == {}, "neither the base's nor the staged values"
    assert _efficiency(staged, "Manchester Gas"), "another entity keeps its own"

    child = root.child(staged.edits)
    assert _efficiency(child.record) == {}
    assert _efficiency(child.record, "Manchester Gas")
    assert "Manchester Wind" not in set(_connections(child.record)["entity"]), (
        "the tuples went with it"
    )


def test_remove_of_no_labels_stages_nothing(staged):
    staged.remove("entity", [])
    assert not staged.edits.axes(), "no tombstone, and no cascade to look up"


def test_remove_then_add_starts_without_the_parents_connections(staged, root):
    """`add` replaces the entity's tombstone, not its connections'.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    - [deletion cascades through tombstones](https://energy-models.github.io/datarecord/design/proposals/deletion-cascades-through-tombstones/)
    """
    assert _efficiency(staged), "the base carries a value to lose"
    staged.remove("entity", ["Manchester Wind"])
    staged.add("entity", ["Manchester Wind"], entity_type=GEN)

    child = root.child(staged.edits)
    assert _entity_axis(child.record).loc["Manchester Wind", "entity_type"] == GEN
    assert _efficiency(child.record) == {}, "its connections are re-added by hand"


def _connections(record):
    return unpacked(record.groups["connection"])


def test_add_group_rejects_flat_coordinates(staged):
    frame = pd.DataFrame([{"entity": "Manchester Wind", "bus": "Norway"}])
    with pytest.raises(ValueError, match="pass 'connection' as one struct column"):
        staged.add_group("connection", frame)


def test_add_group_needs_the_struct_column(staged):
    with pytest.raises(ValueError, match="needs a 'connection' column"):
        staged.add_group("connection", pd.DataFrame([{"role": "attached"}]))


def test_every_declared_group_reads_its_staged_rows(store, ac_dc):
    """A second group is not silently dropped: the reads are keyed by group.

    `connection` is the group every fixture has, so a read path naming it rather
    than iterating the declared ones would pass everywhere except here - a
    record declaring a `corridor` would read nothing while holding rows to
    commit.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    revision = export_network(ac_dc, store)
    write_schema(
        schema(
            groups={
                "connection": {"entity": "entity", "bus": "bus"},
                "corridor": {"from": "entity", "to": "entity"},
            }
        ),
        store,
    )
    staged = WorkingRecord(revision.record)

    staged.add_group(
        "connection",
        pd.DataFrame([{"connection": {"entity": "Manchester Wind", "bus": "Norway"}}]),
    )
    staged.add_group(
        "corridor",
        pd.DataFrame([{"corridor": {"from": "Manchester Wind", "to": "Norway"}}]),
    )

    connections = unpacked(staged.groups["connection"])
    assert "Norway" in set(
        connections[connections["entity"] == "Manchester Wind"]["bus"]
    )

    corridors = unpacked(staged.groups["corridor"], "corridor")
    assert list(zip(corridors["from"], corridors["to"], strict=True)) == [
        ("Manchester Wind", "Norway")
    ], "the second group reads its own rows, not the first's"


# -- rollback (https://energy-models.github.io/datarecord/design/working-record/#edits) --------------------------------------------------------


def test_rollback_discards_everything_staged(staged, root):
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    staged.remove("entity", ["Norway Gas"])
    staged.rollback()

    # The record reads the base rows again, edit and tombstone alike.
    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert 0.42 not in set(rows["value"])
    assert "Norway Gas" in _entity_axis(staged).index, (
        "the tombstone went with the rest"
    )


def test_writing_the_edits_clears_nothing(staged, root):
    """A `WorkingRecord` still holding its edits after a write reads the same.

    Clearing is `rollback()`, called or not, so a second write of the edits
    writes them again and one after `rollback()` writes nothing.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    first = root.child(staged.edits)
    still = PyPSA.build(staged).c[GEN].static["p_nom"]["Manchester Wind"]
    assert still == 150.0, "still staged"

    again = root.child(staged.edits)
    assert list(_layer_axis(again, "entity")["entity"]) == ["Manchester Wind"]
    assert list(_layer_axis(first, "entity")["entity"]) == ["Manchester Wind"]

    staged.rollback()
    emptied = root.child(staged.edits)
    assert _layer_axis(emptied, "entity").empty


# -- commit targets (https://energy-models.github.io/datarecord/design/working-record/#edits) --------------------------------------------------


def test_a_child_layer_holds_only_the_edits(staged, root, store):
    """A patch layer is the edits alone; the fold resolves the rest.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = root.child(staged.edits)

    rows = _layer_axis(child, "entity")
    assert list(rows["entity"]) == ["Manchester Wind"]
    # Yet the resolved record reads every generator's value.
    assert len(_static(child, "p_nom")) > 1


def test_a_completed_axis_carries_the_touched_key_and_no_other(staged, root, store):
    """Owning `snapshot` whole completes the edited series, not every series.

    `p_max_pu` varies over a non-partial `snapshot`, so touching one of
    Manchester Wind's snapshots makes the layer the owner of that generator's
    whole series and it must carry the untouched snapshots too. The scope is the
    key the edit named: a second generator the edit never mentioned stays in the
    parent, and a layer carrying it would claim an extent it was never given.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)
    assert len(mine) > 1, "a one-snapshot series would not distinguish the two scopes"

    staged.set("p_max_pu", one)
    child = root.child(staged.edits)

    rows = _layer_rows(child, "p_max_pu")
    assert set(rows["entity"]) == {"Manchester Wind"}, (
        "only the touched key's extent is carried"
    )
    written = rows.sort_values("snapshot")
    assert written["value"].tolist() == [0.123, *mine.iloc[1:]["value"].tolist()], (
        "the edit, then the rest of the series it now owns"
    )


def test_edits_are_written_only_under_their_base(staged, root, ac_dc, store):
    """A delta against one revision would read back wrong under another.

    An axis edit carries its base's sibling columns, so the edits staged over
    `root` describe a layer over `root` and nothing else.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    other = export_network(ac_dc, store)
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    with pytest.raises(ValueError, match="delta against"):
        other.child(staged.edits)
    assert root.child(staged.edits).parent == root.id
    # The record itself is the flattened whole, which any revision may take.
    assert other.child(staged).parent == other.id


def test_the_edits_land_in_the_child_not_the_node_branched_from(staged, root, store):
    """Layers are write-once, so the parent still reads its own values.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    before = _static(root, "p_nom")["Manchester Wind"]
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = root.child(staged.edits)

    assert _static(child, "p_nom")["Manchester Wind"] == 150.0
    assert _static(root, "p_nom")["Manchester Wind"] == before


def test_an_edit_over_a_directory_base_reads_back(store, ac_dc):
    """A staged edit must win over a directory base, as it does over a layered one.

    The two are separate sources of one fold, and `source_for` tells them apart
    by `layer_uuid` alone - so a base and a staging area sharing an id would
    send every staged win to the base and read the edit back as the base's
    value, with a correct-looking owner map above it. The unstaged fixtures
    cannot see that: it takes an edit that is supposed to displace something.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    revision = export_network(ac_dc, store)
    staged = WorkingRecord(store.layer(revision.id))

    def marginal_cost() -> list[float]:
        frame = staged.attributes["marginal_cost"].collect().to_native().to_pandas()
        return frame[frame["entity"] == "Manchester Wind"]["value"].tolist()

    before = marginal_cost()
    staged.set("marginal_cost", 4242.0, entity=["Manchester Wind"])
    assert marginal_cost() == [4242.0], (
        "the staged layer owns the key, so the read takes its value"
    )
    assert before != [4242.0], "otherwise the assertion above proves nothing"

    # The base's id is derived from its revision's rather than allocated, so
    # two readers of one layer agree on which layer they are reading.
    other = WorkingRecord(store.layer(revision.id))
    assert other._base.revision_id == staged._base.revision_id
    assert other._layer_id != staged._layer_id, "each staging area is its own layer"


def test_edits_over_a_directory_are_no_revisions_delta(store, written_directory):
    """A directory is no node in a tree, so its edits are a child of none.

    Its `revision_id` is the id its manifest carries, so it names no
    revision; the flattened record writes as a standalone directory instead.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    revision = empty_root(store)
    over_a_directory = WorkingRecord(store.workspace.at(written_directory))

    with pytest.raises(ValueError, match="delta against"):
        revision.child(over_a_directory.edits)


def test_a_directory_uri_reads_the_same_with_or_without_a_trailing_slash(
    written_directory, workspace
):
    """Every member path is appended to the URI, so the slash cannot be optional.

    And `layer_id` is the id the manifest under that URI carries, so both
    spellings must reach the one file - otherwise one directory reads as two
    layers and a `WorkingRecord` over each disagrees about which it edits.
    """
    bare = written_directory.rstrip("/")
    assert not bare.endswith("/"), "otherwise this asserts nothing"

    with_slash = workspace.at(bare + "/")
    without = workspace.at(bare)
    assert list(without.attributes) == list(with_slash.attributes)
    assert list(without.attributes), "the fixture must have written attributes"
    assert without.resolver.revision_id == with_slash.resolver.revision_id, (
        "one directory is one layer, however its URI was spelled"
    )


def test_a_standalone_directory_reads_through_another_workspace(staged, tmp_path):
    """A standalone record carries its own schema, so any workspace reads it.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    out = str(tmp_path / "standalone")
    staged.resolver.workspace.write(staged, out)

    elsewhere = Workspace()
    try:
        assert elsewhere.at(out).schema == staged.schema
    finally:
        elsewhere.close()


def test_edits_read_under_the_base_records_schema(staged, workspace, tmp_path):
    """A staging area declares nothing; it edits under the base's declaration.

    Which matters for a standalone base, whose schema is its own directory's
    rather than any store's: the staged layer is one more source over that
    base, so `with_source` has to carry the schema along. A store on the same
    workspace declaring something else must not reach the edit.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    out = str(tmp_path / "standalone")
    workspace.write(staged, out)
    unrelated = workspace.store(str(tmp_path / "unrelated"))
    write_schema(Schema(), unrelated)

    base = workspace.at(out)
    assert base.schema.dims, "the standalone record carries its own schema"
    editing = WorkingRecord(base)
    assert editing.schema == base.schema, "an edit declares nothing of its own"
    editing.set("p_nom", 999.0, entity=["Manchester Wind"])
    assert _entity_axis(editing).loc["Manchester Wind", "p_nom"] == 999.0


def test_a_standalone_write_of_the_record_is_flattened(staged, store, tmp_path):
    """No parent to resolve against, so the whole record is written.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    out = str(tmp_path / "flat")
    record = store.workspace.write(staged, out)
    assert record.schema == staged.schema
    members = _entity_axis(record)
    assert members.loc["Manchester Wind", "p_nom"] == 150.0
    # Flattened: every component is present, not left to a parent to supply.
    assert (members["entity_type"] == GEN).sum() == 6


def test_a_committed_child_builds_a_network(staged, root):
    """The whole point: an edited record is still a buildable model.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = root.child(staged.edits)

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
    staged.set("p", 42.0, entity=["Manchester Wind"], bus="Manchester", kind="outputs")

    rows = _unpacked(staged.outputs["p"])
    assert dict(zip(rows["entity"], rows["value"], strict=True)) == {
        "Manchester Wind": 42.0
    }
    # Staged as a result, so it is not an input.
    assert "p" not in staged.attributes


def test_results_survive_a_commit_into_the_new_layer(staged, root, store):
    """Staged results land in the child's `outputs/`, alongside its inputs.

    Notes
    -----
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    staged.set("p", 42.0, entity=["Manchester Wind"], bus="Manchester", kind="outputs")
    child = root.child(staged.edits)

    rows = _layer_rows(child, "p", kind="outputs")
    assert rows["value"].tolist() == [42.0]
    # And the inputs went where inputs go.
    assert not _layer_axis(child, "entity").empty


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
    with pytest.raises(KeyError, match="NoSuchGenerator"):
        staged.set("p_max_pu", 1.0, entity=["NoSuchGenerator"])


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
    staged.set("p", 42.0, entity=["Manchester Wind"], bus="Manchester", kind="outputs")
    numeric = staged.outputs["p"].collect().to_native().to_pandas()
    assert numeric["value"].tolist() == [42.0], "a numeric result comes back a number"


def test_a_multi_type_results_frame_stages_by_name_alone(staged, root, store):
    """One frame spanning types is one call, keyed by name alone.

    `Tool.results` hands over one frame per attribute carrying every type's rows; with names unique there is no type to stamp, so the frame needs no
    `entity_type` and nothing can be silently relabelled.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    frame = staged.con.sql(
        "SELECT {'entity': entity, 'bus': bus} AS connection, value "
        "FROM (VALUES ('Manchester Wind', 'Manchester', 1.0), ('0', 'Norway', 2.0)) "
        "t(entity, bus, value)"
    )
    staged.set("p", frame, kind="outputs")

    rows = _unpacked(staged.outputs["p"])
    assert dict(zip(rows["entity"], rows["value"], strict=True)) == {
        "Manchester Wind": 1.0,
        "0": 2.0,
    }

    # And it survives the commit into the layer's own `outputs/`.
    child = root.child(staged.edits)
    got = store.layer(child.id).outputs["p"]
    assert set(_unpacked(got)["entity"]) == {"Manchester Wind", "0"}
    assert "entity_type" not in got.collect_schema().names()


def test_a_scalar_derives_the_type_from_the_name(staged):
    """No type keyword: the name determines it, so a bare `set` is enough.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    assert _entity_axis(staged).loc["Manchester Wind", "p_nom"] == 150.0


def test_one_call_spans_component_types(staged):
    """Names decide the type, so one edit may cross types.

    `p_nom` is declared for both `Generator` and `Link`, and each name is
    validated against its own type's spec - one call, two types, no keyword.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged.set("p_nom", {"Manchester Wind": 150.0, "DC link": 80.0})
    got = _entity_axis(staged)["p_nom"]
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
        staged.set(
            "p_max_pu",
            {"Manchester Wind": 0.5, "London Load": 0.5},
            indexed_by="entity",
        )
