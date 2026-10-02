# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`WorkingRecord`: staging, the edit operations, commit.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision, duck
from datarecord.duck import layer_dir
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.layered.revision import Record
from datarecord.layered.sources import ParquetLayer
from datarecord.mutable import Directory, NewChild, WorkingRecord
from datarecord.record import RecordLike
from datarecord.schema import AttributeSpec, Schema
from datarecord.sources import to_sources
from tests.fixtures import export_network, members, names, schema

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


@pytest.fixture
def written_directory(root):
    """One layer's own directory, as a URI a caller could pass to `Record.at`."""
    return layer_dir(root.id)


def _static(revision, attribute, ctype=GEN):
    """One attribute's value per live member of one type, broadcasts expanded."""
    mine = set(names(revision.record, ctype))
    return {
        e: v for e, v in _entity_column(revision.record, attribute).items() if e in mine
    }


def _entity_column(record, attribute):
    """One attribute over `entity`, per entity name, as the record resolves it."""
    frame = to_sources(record, [attribute])[attribute].collect("pandas").to_native()
    return dict(zip(frame["entity"], frame["value"], strict=True))


def _layer_rows(revision, attribute, con):
    """One committed layer's own `attributes/<attribute>.parquet`, as pandas.

    The single-layer view, read through the `LayerSource` for that layer rather
    than the folding resolver: "what did this patch write" is a question about
    one layer's file, not the resolved record, so `Record.at` - which folds a
    source through the whole-tree machinery - is the wrong lens for it.
    """
    rel = ParquetLayer(revision.id, read_schema(con), con).attribute(attribute)
    return rel.to_df() if rel is not None else pd.DataFrame()


# -- the protocol (https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit) ----------------------------------------------------


def test_a_working_record_overrides_no_read_member():
    """Every member it defines beyond `Record`'s is an edit, a commit, or private.

    The property the whole design rests on: a staged edit is read by the same
    fold that reads a committed layer, so a *read* member `WorkingRecord`
    redefines is a place where staging stopped being just another layer. No
    behavioural test would name that, because both paths would still answer -
    they would just be two paths again.
    """
    edits = {
        "set",
        "add",
        "remove",
        "add_relation",
        "remove_relation",
        "rollback",
        "commit",
    }
    inherited = {n for n in vars(Record) if not n.startswith("_")}
    defined = {n for n in vars(WorkingRecord) if not n.startswith("_")}
    assert inherited & defined == set(), (
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


@pytest.mark.parametrize(
    "value",
    [
        pytest.param({"Manchester Wind": 1.0}, id="mapping"),
        pytest.param([1.0], id="sequence"),
        pytest.param(pd.Series({"Manchester Wind": 1.0}), id="series"),
    ],
)
def test_a_value_per_label_is_a_frame(staged, value):
    """`set` takes a scalar, a long frame or an `nw.Expr`; the error names the frame."""
    with pytest.raises(
        TypeError, match=r"pd\.DataFrame\(\{'entity': \[\.\.\.\], 'snapshot'"
    ):
        staged.set("p_max_pu", value)
    assert "p_max_pu" not in staged.resolver.sources[-1].attributes(), "nothing staged"


# -- set (https://energy-models.github.io/datarecord/design/working-record/#set) -------------------------------------------------------------


def test_set_stages_without_writing(staged, root):
    """Staging is not a layer: the record reads the edit, the record does not."""
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    assert _entity_column(staged, "p_nom")["Manchester Wind"] == 150.0
    assert _static(root, "p_nom")["Manchester Wind"] != 150.0, (
        "the record itself is untouched until commit"
    )


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
    """Two edits to one key leave the later one, read back before commit too.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 100.0, entity=["Manchester Wind"])
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    rows = staged.attributes["p_nom"].collect("pandas").to_native()
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


@pytest.mark.parametrize(
    ("edit", "match"),
    [
        pytest.param(
            lambda s: s.set(
                "p_max_pu",
                0.4,
                entity="Manchester Wind",
                snapshot=["2030-01-01", "2030-01-02"],
            ),
            r"snapshot is Datetime, and '2030-01-01' is a str \(2 labels\)",
            id="str-for-datetime-keyword-two-labels",
        ),
        pytest.param(
            lambda s: s.set(
                "p_max_pu", 0.4, entity="Manchester Wind", snapshot="2030-01-01"
            ),
            r"snapshot is Datetime, and '2030-01-01' is a str",
            id="str-for-datetime-keyword",
        ),
        pytest.param(
            lambda s: s.set(
                "p_max_pu", 0.4, entity="Manchester Wind", snapshot=["2030-01-01"]
            ),
            r"snapshot is Datetime, and '2030-01-01' is a str",
            id="str-for-datetime-keyword-list",
        ),
        pytest.param(
            lambda s: s.set("p_max_pu", 0.4, entity="Manchester Wind", snapshot=5),
            r"snapshot is Datetime, and 5 is an int; pass a pd\.Timestamp",
            id="int-for-datetime-keyword",
        ),
        pytest.param(
            lambda s: s.set("p_max_pu", 0.4, entity=[1]),
            r"entity is String, and 1 is an int; pass str\(1\)",
            id="int-for-string-keyword-list",
        ),
        pytest.param(
            lambda s: s.remove("entity", [1]),
            r"entity is String, and 1 is an int",
            id="int-for-string-remove",
        ),
        pytest.param(
            lambda s: s.remove_relation("connection", [(1, "London")]),
            r"entity is String, and 1 is an int",
            id="int-for-string-remove-relation",
        ),
    ],
)
def test_a_label_of_another_type_than_its_dim_is_refused(staged, edit, match):
    """A label is checked against its dim's dtype before any relation is built.

    Each of these reached the builder and failed there - as pyarrow's `object of
    type <class 'str'> cannot be converted to int` or `Expected bytes, got a
    'int' object`, or DuckDB's `Unimplemented type for cast (INTEGER ->
    TIMESTAMP)` - naming neither the dim nor what it declares. A scalar
    keyword `snapshot="2030-01-01"` did not fail at all: DuckDB parsed it as a
    date, a guess the list form of the same call refused.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(TypeError, match=match):
        edit(staged)


def test_set_refuses_a_dim_an_attribute_is_not_over(staged):
    """`p_nom` is over `entity` alone, so it has no `scenario` to scope.

    The entity-axis path of `set` dropped the keyword and wrote the value for
    every scenario, where the long path refuses it.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(ValueError, match="does not vary over"):
        staged.set("p_nom", 200.0, entity=["Manchester Wind"], scenario="high")


def test_a_scalar_reaches_an_entity_staged_by_add(staged):
    """`entity=None` means every entity the record resolves, pending adds included.

    The entity-axis path broadcast a scalar over the base's labels only, so an
    entity staged by `add` kept no value.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged.add("entity", pd.DataFrame({"entity": ["new wind"], "entity_type": [GEN]}))
    staged.set("p_nom", 5.0)
    got = _entity_column(staged, "p_nom")
    assert got["new wind"] == got["Manchester Wind"] == 5.0, (
        "a scalar with no `entity=` reaches staged and base entities alike"
    )


def test_set_accepts_a_name_staged_by_add(staged, root):
    """`add` makes the name exist, so a value for it is no longer unknown.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    staged.add(
        "entity",
        pd.DataFrame([{"entity": "NewSolar", "entity_type": GEN, "carrier": "solar"}]),
    )
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


@pytest.mark.parametrize(
    "scope",
    [
        pytest.param({"entity": ["Manchester Wind"]}, id="listed-label"),
        pytest.param({"entity": "Manchester Wind"}, id="one-label"),
        pytest.param({}, id="every-label"),
    ],
)
def test_an_expression_derives_an_attribute_over_one_dim(staged, root, scope):
    """`p_nom` is over `entity` alone, and derives from its current value.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    before = _entity_column(staged, "p_nom")
    targets = {"Manchester Wind"} if scope else set(before)
    want = {
        name: value * 2 if name in targets else value for name, value in before.items()
    }

    staged.set("p_nom", nw.col("value") * 2, **scope)
    assert _entity_column(staged, "p_nom") == pytest.approx(want, nan_ok=True), (
        "the targets doubled and every other label kept its value"
    )

    child = staged.commit(NewChild(root))
    assert _entity_column(child.record, "p_nom") == pytest.approx(want, nan_ok=True)


def test_two_expressions_compose(staged):
    """The second derived edit reads the first one's staged value, not the base's.

    The derived frame was a relation over the staging table, and the insert
    deletes the rows it replaces first, so the second edit re-read the base and
    returned 81.0 where 161.0 was due.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    before = _entity_column(staged, "p_nom")["Manchester Wind"]

    staged.set("p_nom", nw.col("value") * 2, entity=["Manchester Wind"])
    staged.set("p_nom", nw.col("value") + 1, entity=["Manchester Wind"])
    assert _entity_column(staged, "p_nom")["Manchester Wind"] == before * 2 + 1


def test_an_expression_refuses_a_dim_the_attribute_is_not_over(staged):
    """`p_nom` has no `scenario` to scope, so the derived form refuses it too.

    It raised narwhals' "The selected columns were not found" instead.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(ValueError, match="does not vary over"):
        staged.set(
            "p_nom",
            nw.col("value") * 2,
            entity=["Manchester Wind"],
            scenario="high",
        )


def test_an_expression_on_a_label_with_no_value_raises(staged):
    """A label with no `p_nom` row has nothing to derive from.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    staged.add("entity", pd.DataFrame([{"entity": "NewWind"}]))
    with pytest.raises(KeyError, match="no current value to derive from"):
        staged.set("p_nom", nw.col("value") * 2, entity=["NewWind"])


def test_flags_report_a_dim_a_staged_edit_introduces(staged, ac_dc):
    """A staged row's dims join the flags, unioned with the base answer.

    `flags` is the one non-`Frames` member of `Record`, so the promise that a
    read reflects pending edits has to hold for it too - and it decides which
    container a consumer puts a value in (a constant or a series). `marginal_cost` starts broadcast over `snapshot`
    and varying over nothing; a per-snapshot edit must add `snapshot` to
    `varies` while leaving `broadcast` alone, since the base's NULL-snapshot
    rows are still there.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """
    before = staged.flags(entity=names(staged, GEN))["marginal_cost"]
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

    after = staged.flags(entity=names(staged, GEN))["marginal_cost"]
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
        return _entity_column(staged, "p_nom")[name]

    staged.set("p_nom", 11.0, entity=["Manchester Wind"])
    assert p_nom("Manchester Wind") == 11.0

    staged.set("p_nom", 22.0, entity=["Manchester Wind"])
    assert p_nom("Manchester Wind") == 22.0, "the read re-folds the staged layer"

    staged.set("p_nom", 33.0, entity=["Norway Wind"])
    assert p_nom("Manchester Wind") == 22.0, (
        "re-folding keeps a key the latest edit never named"
    )
    assert p_nom("Norway Wind") == 33.0


def test_flags_do_not_depend_on_the_maps_grouping_grain(staged, ac_dc):
    """The fold flags per key; `flags` unions across the entities. Both must agree.

    The staged and parquet paths share one aggregate exactly because the
    grouping grain is invisible to the answer: the map computes `varies` and
    `broadcast` per owner-map key, and `flags` `bool_or`-unions them over the
    entities it is asked about. So entities staged at different grains - one
    per-snapshot, one broadcast - must report both sets, the same answer either
    grouping would give.

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
        entity=["Manchester Wind"],
    )
    staged.set("marginal_cost", 3.0, entity=["Norway Wind"])

    flags = staged.flags(entity=names(staged, GEN))["marginal_cost"]
    assert "snapshot" in flags.varies, "one member's rows name the snapshot"
    assert "snapshot" in flags.broadcast, "another's leave it NULL"


# -- value dtypes (https://energy-models.github.io/datarecord/design/record/#flags, https://energy-models.github.io/datarecord/design/schema/#attributespec) -----------------------------------------------


def test_a_non_float_attribute_stages_and_commits(root, con):
    """`value` carries the attribute's declared dtype, not always `DOUBLE`.

    One staging table holds every attribute's values, so it stages `value` as
    text and casts to the declared dtype where the attribute is known - which
    is the point at which `attributes/<attr>.parquet` is per-attribute.

    The schema is amended before the `WorkingRecord` is built: a record carries
    the schema its base was resolved under, so a widening has to be in force
    when the record is constructed, not slipped in before a later commit.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    amended = read_schema()
    amended.attributes["carrier"] = AttributeSpec(
        dtype=nw.String(), dims={"entity", "scenario"}
    )
    write_schema(amended)

    staged = WorkingRecord(root.record, con)
    staged.set("carrier", "solar", entity=["Manchester Wind"])
    rows = staged.attributes["carrier"].collect().to_native().to_pandas()
    assert rows[rows["entity"] == "Manchester Wind"]["value"].tolist() == ["solar"]

    child = staged.commit(NewChild(root))
    got = child.record.attributes["carrier"].collect().to_native().to_pandas()
    assert got[got["entity"] == "Manchester Wind"]["value"].tolist() == ["solar"]


def test_a_float_attribute_stays_numeric(staged):
    """The cast is per attribute, so a `DOUBLE` one is not turned into text.

    `p_max_pu` because it is a long attribute: the one staging table holding
    its rows is where a value is held as text.
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert rows["value"].dtype.kind == "f"
    assert 0.42 in set(rows["value"])


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

    rows = _layer_rows(child, "p_max_pu", con)
    assert len(rows[rows["entity"] == "Manchester Wind"]) == len(mine)
    # Only the touched component: an axis owned whole obliges the layer to
    # carry that key's extent, not every key's.
    assert set(rows["entity"]) == {"Manchester Wind"}


def test_two_edits_on_one_whole_owned_series_both_land(staged, root, con):
    """A second `set` replaces its own fill without disturbing the first edit's.

    The case `staging-without-seq` leans on hardest: a `set` on a whole-owned
    axis stages the touched value plus the untouched coordinates as fills, and a
    second `set` at another coordinate must replace only the fill on *its* key -
    the first edit's value and its fills have to survive. Idempotence of
    `_complete_owned_whole` and replace-by-coordinate are what make it hold with
    no ordering column to rank a fill below an edit.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(mine) > 2, "the series needs an untouched middle to keep"

    first = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.111)
    second = mine.iloc[[1]][["entity", "snapshot"]].assign(value=0.222)
    staged.set("p_max_pu", first, entity=["Manchester Wind"])
    staged.set("p_max_pu", second, entity=["Manchester Wind"])

    child = staged.commit(NewChild(root))
    rows = _layer_rows(child, "p_max_pu", con)
    got = rows[rows["entity"] == "Manchester Wind"].sort_values("snapshot")

    assert len(got) == len(mine), "the whole extent is carried, once per snapshot"
    assert got.iloc[0]["value"] == 0.111, "the first edit survives the second"
    assert got.iloc[1]["value"] == 0.222, "the second edit replaced its own fill"
    assert got.iloc[2:]["value"].tolist() == mine.iloc[2:]["value"].tolist(), (
        "every untouched snapshot keeps the base value"
    )


def test_a_partial_axis_stays_a_patch(staged, root, con):
    """`scenario` *is* partial, so one value may be patched alone.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    assert staged.schema.owned_per("p_nom") == frozenset({"entity"}), (
        "owned per entity alone, so there is no extent along another axis"
    )
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    rows = _layer_rows(child, "p_nom", con)
    assert list(rows["entity"]) == ["Manchester Wind"], (
        "no extent to restate, so the layer holds the one edited row"
    )


# -- add and remove (https://energy-models.github.io/datarecord/design/working-record/#add-remove) --------------------------------------------------


def test_add_then_commit_makes_a_component_exist(staged, root):
    staged.add(
        "entity",
        pd.DataFrame(
            [
                {
                    "entity": "NewSolar",
                    "entity_type": GEN,
                    "bus": "Manchester",
                    "carrier": "solar",
                    "p_nom": 42.0,
                }
            ]
        ),
    )
    assert "NewSolar" in names(staged, GEN), "the addition reads back before commit"

    child = staged.commit(NewChild(root))
    assert "NewSolar" in set(child.resolver.dims.axes["entity"].df()["entity"])

    assert _static(child, "p_nom")["NewSolar"] == 42.0
    assert _static(child, "carrier")["NewSolar"] == "solar"


def test_add_accepts_a_name_of_its_own_type(staged, root):
    """`add` of a name the record already holds is an edit to that entity."""
    staged.add("entity", pd.DataFrame([{"entity": "Manchester Wind", "p_nom": 5.0}]))
    child = staged.commit(NewChild(root))
    assert _static(child, "p_nom")["Manchester Wind"] == 5.0


def test_require_labels_rejects_a_name_no_layer_declares(staged):
    """A value keyed to a name with no entity-axis row is caught, not dropped.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    with pytest.raises(KeyError, match="Nowhere"):
        staged._require_labels("entity", ["Manchester Wind", "Nowhere"])


def test_add_routes_a_port_attribute_to_the_connections(staged, root):
    """`bus` keys a connection rather than being a member column.

    Putting it in `dims/entity.parquet` would introduce a column the ancestors'
    files lack, which then reads as NULL for their rows - so every existing
    component would lose its bus.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.add(
        "entity",
        pd.DataFrame(
            [
                {
                    "entity": "NewSolar",
                    "entity_type": GEN,
                    "bus": "Manchester",
                }
            ]
        ),
    )
    child = staged.commit(NewChild(root))

    rows = child.record.relations["connection"].collect().to_native().to_pandas()
    buses = dict(zip(rows["entity"], rows["bus"], strict=True))
    assert buses["NewSolar"] == "Manchester"
    assert buses["Manchester Wind"] == "Manchester", (
        "the inherited components keep their bus"
    )


def test_add_stages_a_port_with_its_role_and_relations(staged, root):
    """A port is a label: `role` is a value over it, entity and bus its relations.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
    """
    staged.add(
        "port",
        pd.DataFrame(
            [
                {
                    "port": "Manchester Wind:x",
                    "entity": "Manchester Wind",
                    "bus": "Norway",
                    "role": "attached",
                }
            ]
        ),
    )
    staged.set("efficiency", 0.5, port="Manchester Wind:x")
    child = staged.commit(NewChild(root))

    roles = to_sources(child.record, ["role"])["role"].collect("pandas").to_native()
    assert (
        dict(zip(roles["port"], roles["value"], strict=True))["Manchester Wind:x"]
        == "attached"
    ), "a `role` row over the new port"
    for relation, column, label in (
        ("port_entity", "entity", "Manchester Wind"),
        ("port_bus", "bus", "Norway"),
    ):
        rows = child.record.relations[relation].collect().to_native().to_pandas()
        mapped = dict(zip(rows["port"], rows[column], strict=True))
        assert mapped["Manchester Wind:x"] == label, f"a `{relation}` row"
    efficiency = child.record.attributes["efficiency"].collect().to_native()
    mine = efficiency.to_pandas().query("port == 'Manchester Wind:x'")
    assert list(mine["value"]) == [0.5], "a value over the new port"


def test_add_rejects_a_column_the_schema_does_not_declare(staged):
    """A member column no declaration accounts for is an error, not a new column.

    A staging table is shaped like the file it becomes, from the schema, so an
    undeclared column has no dtype to be given it - widening to fit would have
    to guess one from the caller's frame, and a float guessed as `VARCHAR`
    stored `'1234.5'` for every later reader. A tool that grows a column
    declares it first.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    assert "capex" not in staged.schema.attributes, "undeclared, which is the case here"
    with pytest.raises(ValueError, match="capex"):
        staged.add("entity", pd.DataFrame([{"entity": "NewSolar", "capex": 1234.5}]))


def test_add_fills_a_declared_column_another_add_omitted(staged):
    """A frame omitting a declared attribute stages no value for it rather than failing.

    An `add` naming a subset of the declared attributes is ordinary: what it did
    not carry has no row, and an earlier `add` is unaffected by a later one.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    staged.add("entity", pd.DataFrame([{"entity": "NewSolar", "p_nom": 1234.5}]))
    staged.add("entity", pd.DataFrame([{"entity": "NewWind"}]))

    p_nom = _entity_column(staged, "p_nom")
    assert "NewWind" not in p_nom, "not carried, so no value"
    assert p_nom["NewSolar"] == 1234.5, "unaffected by the later add"


def test_remove_tombstones_without_enumerating_attributes(staged, root):
    staged.remove("entity", ["Norway Gas"])
    assert "Norway Gas" not in names(staged, GEN), "the removal reads back at once"

    child = staged.commit(NewChild(root))
    assert "Norway Gas" not in set(child.resolver.dims.axes["entity"].df()["entity"])


def test_add_after_remove_leaves_the_component_alive(staged, root):
    """The collapse is per key by `_seq`, so the later `add` wins.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.remove("entity", ["Norway Gas"])
    staged.add(
        "entity",
        pd.DataFrame([{"entity": "Norway Gas", "entity_type": GEN, "carrier": "gas"}]),
    )

    child = staged.commit(NewChild(root))
    assert "Norway Gas" in set(child.resolver.dims.axes["entity"].df()["entity"])


def test_a_tombstone_drops_that_components_staged_attributes(staged, root):
    """Removing a component discards values staged for it, via the anti-join.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 99.0, entity=["Norway Gas"])
    staged.remove("entity", ["Norway Gas"])

    child = staged.commit(NewChild(root))
    assert "Norway Gas" not in _static(child, "p_nom")


def _relation_map(record, relation):
    """One functional relation as a dict, its key label to its `values` label."""
    (key,) = record.schema.relation_key(relation)
    values = record.schema.relations[relation].values
    rows = record.relations[relation].collect().to_native().to_pandas()
    return dict(zip(rows[key], rows[values], strict=True))


@pytest.mark.parametrize(
    "committed",
    [pytest.param(False, id="staged"), pytest.param(True, id="committed")],
)
def test_a_removed_label_takes_the_relation_rows_that_map_to_it(
    staged, root, committed
):
    """`port_entity` maps a port to its entity, so removing the entity removes that row.

    The row is keyed on the port, not on the entity, so the key-side cascade
    alone left it behind, mapping the port to an entity that no longer exists.
    The port itself stays: removing it is `remove("port", ...)`.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    port = "Norway Gas:"
    assert _relation_map(staged, "port_entity")[port] == "Norway Gas", (
        "the fixture attaches Norway Gas through this port"
    )

    staged.remove("entity", ["Norway Gas"])
    record = staged.commit(NewChild(root)).record if committed else staged

    assert "Norway Gas" not in set(_relation_map(record, "port_entity").values()), (
        "no `port_entity` row maps a port to the removed entity"
    )
    ports = record.dims["port"].collect("pandas").to_native()
    assert port in set(ports["port"]), "the port stays on its axis"


@pytest.mark.parametrize(
    ("relation", "axis"),
    [
        pytest.param("port_bus", None, id="values-dim-without-an-axis"),
        pytest.param("entity_type", GEN, id="values-dim-outside-partial"),
    ],
)
def test_a_removed_label_leaves_a_relation_whose_values_cannot_lose_one(
    staged, root, relation, axis
):
    """Only the rows keyed on the removed entity go, when `values` has no label to lose.

    `bus` is `partial` but has no axis, so no label of it is removed and a row
    naming one stands. `entity_type` is outside `partial`, so a layer owns its
    axis whole and a label is never removed from it; an axis listing only
    `Generator` does not remove the rows naming other types.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    before = _relation_map(staged, relation)
    values = staged.schema.relations[relation].values
    if axis is not None:
        staged.add(values, pd.DataFrame({values: [axis]}))

    staged.remove("entity", ["Norway Gas"])
    after = _relation_map(staged.commit(NewChild(root)).record, relation)

    assert after == {k: v for k, v in before.items() if k != "Norway Gas"}, (
        f"`{relation}` loses only the rows keyed on Norway Gas"
    )


# -- connect and disconnect (https://energy-models.github.io/datarecord/design/working-record/#add-remove, https://energy-models.github.io/datarecord/design/record/#connections) --------------------------------------


def test_add_relation_stages_a_new_connection(staged, root):
    """A connection is a row keyed by `(name, bus)`, not a positional column.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.add_relation(
        "connection",
        pd.DataFrame([{"entity": "Manchester Wind", "bus": "Norway"}]),
    )
    staged_rows = staged.relations["connection"].collect().to_native().to_pandas()
    assert "Norway" in set(
        staged_rows[staged_rows["entity"] == "Manchester Wind"]["bus"]
    ), "the new connection reads back before commit"

    child = staged.commit(NewChild(root))
    rows = child.resolver.relation_frame("connection").df()
    got = set(rows[rows["entity"] == "Manchester Wind"]["bus"])
    assert "Norway" in got


def test_remove_relation_stages_a_tombstone(staged, root):
    """One `deleted` row per `(entity, bus)`, the relation's own key.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    staged.remove_relation("connection", [("Norwich Converter", "Norwich")])
    before = staged.relations["connection"].collect().to_native().to_pandas()
    ports = set(before[before["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in ports, "the removal reads back before commit"
    assert "Norwich DC" in ports, "deletion is per connection, not per component"

    child = staged.commit(NewChild(root))
    rows = child.resolver.relation_frame("connection").df()
    left = set(rows[rows["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in left
    # The component's other port survives: deletion is per connection, not per
    # component (https://energy-models.github.io/datarecord/design/record/#connections).
    assert "Norwich DC" in left


def test_add_relation_needs_every_coordinate(staged):
    with pytest.raises(ValueError, match="'bus'"):
        staged.add_relation("connection", pd.DataFrame([{"entity": "Manchester Wind"}]))


def test_every_declared_relation_reads_its_staged_rows(con, base_uri, ac_dc):
    """A second relation is not silently dropped: the reads are keyed by relation.

    `connection` is the relation every fixture has, so a read path naming it rather
    than iterating the declared ones would pass everywhere except here - a
    record declaring a `corridor` would read nothing while holding rows to
    commit.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_schema(
        schema(
            relations={
                "connection": {"entity": "entity", "bus": "bus"},
                "corridor": {"from": "entity", "to": "entity"},
            }
        )
    )
    staged = WorkingRecord(revision.record, con)

    staged.add_relation(
        "connection", pd.DataFrame([{"entity": "Manchester Wind", "bus": "Norway"}])
    )
    staged.add_relation(
        "corridor", pd.DataFrame([{"from": "Manchester Wind", "to": "Norway"}])
    )

    connections = staged.relations["connection"].collect().to_native().to_pandas()
    assert "Norway" in set(
        connections[connections["entity"] == "Manchester Wind"]["bus"]
    )

    corridors = staged.relations["corridor"].collect().to_native().to_pandas()
    assert list(zip(corridors["from"], corridors["to"], strict=True)) == [
        ("Manchester Wind", "Norway")
    ], "the second relation reads its own rows, not the first's"


# -- rollback (https://energy-models.github.io/datarecord/design/working-record/#committing) --------------------------------------------------------


def test_rollback_discards_everything_staged(staged, root):
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    staged.remove("entity", ["Norway Gas"])
    staged.rollback()

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert 0.42 not in set(rows["value"]), "the edit went"
    assert "Norway Gas" in names(staged, GEN), "the tombstone went with the rest"


def test_commit_clears_the_staging_area(staged, root):
    """A second commit writes nothing: the edits left with the first.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    staged.commit(NewChild(root))

    again = staged.commit(NewChild(root))
    layer = Record.at(layer_dir(again.id), con=staged.con)
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

    rows = _layer_rows(child, "p_nom", con)
    assert list(rows["entity"]) == ["Manchester Wind"], "the edited entity alone"
    assert len(_static(child, "p_nom")) > 1, (
        "yet the resolved record reads every generator's value"
    )


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

    rows = _layer_rows(child, "p_max_pu", con)
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


def test_an_edit_over_a_directory_base_reads_back(con, base_uri, ac_dc):
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
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    staged = WorkingRecord(Record.at(layer_dir(revision.id), con), con)

    def marginal_cost() -> list[float]:
        frame = staged.attributes["marginal_cost"].collect().to_native().to_pandas()
        return frame[frame["entity"] == "Manchester Wind"]["value"].tolist()

    before = marginal_cost()
    staged.set("marginal_cost", 4242.0, entity=["Manchester Wind"])
    assert marginal_cost() == [4242.0], (
        "the staged layer owns the key, so the read takes its value"
    )
    assert before != [4242.0], "otherwise the assertion above proves nothing"

    # The base's id is derived from where it is rather than allocated, so two
    # readers of one directory agree on which layer they are reading.
    other = WorkingRecord(Record.at(layer_dir(revision.id), con), con)
    assert other._base.revision_id == staged._base.revision_id
    assert other._layer_id != staged._layer_id, "each staging area is its own layer"


def test_new_child_without_a_layered_base_says_what_to_pass(con, base_uri, tmp_path):
    """A directory is no node in a tree, so there is nothing to branch from.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    revision = Revision.create(con)
    write_schema(read_schema(con), base_uri)
    over_a_directory = WorkingRecord(Record.at(layer_dir(revision.id), con), con)

    with pytest.raises(ValueError, match="needs a revision to branch from"):
        over_a_directory.commit(NewChild())


def test_a_directory_uri_reads_the_same_with_or_without_a_trailing_slash(
    written_directory, con
):
    """Every member path is appended to the URI, so the slash cannot be optional.

    And `layer_id` is derived from that URI, so the two spellings have to
    normalise to one before it is hashed - otherwise one directory reads as two
    layers and a `WorkingRecord` over each disagrees about which it edits.
    """
    bare = written_directory.rstrip("/")
    assert not bare.endswith("/"), "otherwise this asserts nothing"

    with_slash = Record.at(bare + "/", con)
    without = Record.at(bare, con)
    assert list(without.attributes) == list(with_slash.attributes)
    assert list(without.attributes), "the fixture must have written attributes"
    assert without.resolver.revision_id == with_slash.resolver.revision_id, (
        "one directory is one layer, however its URI was spelled"
    )


def test_edits_read_under_the_base_records_schema(staged, con, tmp_path):
    """A staging area declares nothing; it edits under the base's declaration.

    Which matters for a standalone base, whose schema is its own directory's
    rather than the connection root's: the staged layer is one more source over
    that base, so `with_source` has to carry the schema along. Reading the edit
    back under the connection's schema instead would resolve it against
    different dims than the rows it is overlaying.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    out = str(tmp_path / "standalone")
    staged.commit(Directory(out))

    elsewhere = duck.connect(base_uri=str(tmp_path / "unrelated"))
    try:
        assert read_schema(elsewhere) == Schema(), "the other root declares nothing"
        base = Record.at(out, elsewhere)
        assert base.schema.dims, "the standalone record carries its own schema"

        editing = WorkingRecord(base, elsewhere)
        assert editing.schema == base.schema, "an edit declares nothing of its own"
        editing.set("p_nom", 999.0, entity=["Manchester Wind"])
        assert _entity_column(editing, "p_nom")["Manchester Wind"] == 999.0
    finally:
        elsewhere.close()


def test_a_directory_target_writes_a_flattened_record(staged, root, con, tmp_path):
    """No parent to resolve against, so the whole record is written.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    out = str(tmp_path / "flat")
    assert staged.commit(Directory(out)) is None

    record = Record.at(out, con)
    assert _entity_column(record, "p_nom")["Manchester Wind"] == 150.0
    assert len(members(record, GEN)) == 6, (
        "flattened: every generator is present, not left to a parent to supply"
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


def _manchester_wind(record, attribute):
    """Manchester Wind's `attribute` values, in coordinate order."""
    rows = record.attributes[attribute].collect("pandas").to_native()
    rows = rows[rows["entity"] == "Manchester Wind"]
    return rows.sort_values(list(rows.columns[:-3]))["value"].tolist()


@pytest.mark.parametrize(
    "attribute",
    [
        pytest.param("p_max_pu", id="long-attribute"),
        pytest.param("p_nom", id="entity-attribute"),
    ],
)
def test_an_expression_naming_one_label_with_no_value_raises(staged, attribute):
    """Every named label must have a value to derive from, not just one of them.

    `NewWind` holds no value, so the call failed to change it. It raised only
    where no named label had a value, so here it doubled Manchester Wind and
    skipped `NewWind` without a word. A failed derived `set` stages nothing.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    staged.add("entity", pd.DataFrame([{"entity": "NewWind"}]))
    before = _manchester_wind(staged, attribute)

    with pytest.raises(KeyError, match=r"no current value to derive from") as raised:
        staged.set(
            attribute, nw.col("value") * 2, entity=["Manchester Wind", "NewWind"]
        )
    assert "NewWind" in str(raised.value), "the message names the label with no value"
    assert "Manchester Wind" not in str(raised.value), (
        "the message names only the labels with no value"
    )
    assert _manchester_wind(staged, attribute) == before, (
        "a failed derived set stages nothing"
    )


def test_an_unscoped_expression_over_an_absent_attribute_stages_nothing(root, con):
    """`entity=None` and no scope means "whatever resolves", so empty is an answer.

    The attribute is declared here because `export_network` writes a row for
    every attribute it declares, so the record has none that resolves empty.
    """
    amended = read_schema()
    amended.attributes["absent"] = AttributeSpec(
        dtype=nw.Float64(), dims={"entity", "snapshot"}
    )
    write_schema(amended)
    staged = WorkingRecord(root.record, con)
    assert "absent" not in staged.attributes, "declared, yet no layer holds a row"

    staged.set("absent", nw.col("value") * 2)
    assert "absent" not in staged.attributes, "nothing resolved, so nothing was staged"


def test_a_long_frame_spanning_types_stages_by_name_alone(staged, root, con):
    """One frame spanning types is one call, keyed by name alone.

    A row is keyed by `entity`, so a frame carrying a `Generator`'s and a
    `Link`'s rows needs no `entity_type`, and it survives the commit.
    """
    frame = pd.DataFrame(
        [
            {"entity": "Manchester Wind", "value": 1.0},
            {"entity": "DC link", "value": 2.0},
        ]
    )
    staged.set("marginal_cost", frame)

    rows = staged.attributes["marginal_cost"].collect().to_native().to_pandas()
    mine = rows[rows["entity"].isin(["Manchester Wind", "DC link"])]
    assert dict(zip(mine["entity"], mine["value"], strict=True)) == {
        "Manchester Wind": 1.0,
        "DC link": 2.0,
    }, "each entity reads back the value its row gave, whatever its type"

    child = staged.commit(NewChild(root))
    got = (
        Record.at(layer_dir(child.id), con)
        .attributes["marginal_cost"]
        .collect()
        .to_native()
        .to_pandas()
    )
    assert set(got["entity"]) == {"Manchester Wind", "DC link"}, (
        "the child's own layer holds both types' rows"
    )
    assert "entity_type" not in got.columns


def test_one_call_spans_component_types(staged):
    """One edit may cross types: an attribute is declared once, over `entity`.

    `Manchester Wind` is a `Generator` and `DC link` a `Link`, and `p_nom` is
    one attribute over `entity` for both - one call, two types, no keyword.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged.set(
        "p_nom",
        pd.DataFrame(
            {"entity": ["Manchester Wind", "DC link"], "value": [150.0, 80.0]}
        ),
    )
    got = _entity_column(staged, "p_nom")
    assert got["Manchester Wind"] == 150.0
    assert got["DC link"] == 80.0
