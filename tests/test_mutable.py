# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`WorkingRecord`: staging, the edit operations, writing the edits.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/use/editing/)
"""

import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from datarecord.mutable import WorkingRecord, normalise_value
from datarecord.resolve.coords import Coords
from datarecord.resolve.record import Record
from datarecord.resolve.sources import ParquetLayer
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from tests.fixtures import (
    broadcast,
    empty_root,
    schema,
    unpacked,
)

GEN = "Generator"


@pytest.fixture(scope="module")
def root(ac_dc_root):
    """`ac_dc` exported and materialised, to branch edits from."""
    return ac_dc_root


@pytest.fixture(scope="module")
def _over_root(root):
    """One staging area over `root` for the module, rolled back after each test."""
    return WorkingRecord(root.record)


@pytest.fixture
def staged(_over_root):
    yield _over_root
    _over_root.rollback()


@pytest.fixture(scope="module")
def small(module_store):
    """`_small` once for the module; a test only ever branches a child of it."""
    return _small(module_store)


@pytest.fixture(scope="module")
def written_directory(small, tmp_path_factory):
    """`small` written standalone, as a URI a caller could pass to `Workspace.at`."""
    out = str(tmp_path_factory.mktemp("written") / "standalone")
    small.store.workspace.write(small.record, out)
    return out


def _small(store, **declared):
    """A root of two generators with a `p_nom`, under `schema(**declared)`.

    `Norway` is both an entity and a `bus` label, as a load and its bus are in `ac_dc`.
    """
    revision = empty_root(store, schema(**declared)).commit()
    staged = WorkingRecord(revision.record)
    staged.add("entity", ["Manchester Wind", "Norway Wind"], ctype=GEN)
    staged.add("entity", ["Norway"], ctype="Bus")
    staged.add("bus", ["Norway"])
    staged.set("p_nom", [80.0, 100.0], entity=["Manchester Wind", "Norway Wind"])
    return revision.store.commit(staged)


def _layer_rows(revision, attribute):
    """One committed layer's own `attributes/<attribute>.parquet`, as pandas.

    The single-layer view, read through the `LayerSource` for that layer rather
    than the folding record: "what did this patch write" is a question about
    one layer's file, not the resolved record, so `Store.layer` - which folds a
    source through the whole-tree machinery - is the wrong lens for it.
    """
    rel = ParquetLayer(revision.manifest, revision.store).attribute(attribute)
    return rel.to_df() if rel is not None else pd.DataFrame()


def _layer_axis(revision, dim):
    """One committed layer's own `dims/<dim>.parquet`, as pandas - `_layer_rows` for an axis."""
    rel = ParquetLayer(revision.manifest, revision.store).axis(dim)
    return rel.to_df() if rel is not None else pd.DataFrame()


# -- the protocol (https://energy-models.github.io/datarecord/design/use/editing/#the-shape-of-an-edit) ----------------------------------------------------


def test_a_working_record_overrides_no_read_member():
    """Every member it defines beyond `Record`'s is an edit, `edits`, or private.

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
        "rollback",
        "edits",
    }
    inherited = {n for n in vars(Record) if not n.startswith("_")}
    defined = {n for n in vars(WorkingRecord) if not n.startswith("_")}
    assert not inherited & defined, (
        "a read member redefined here means staging is no longer just a layer"
    )
    assert defined - inherited == edits, (
        "a public member that is neither an edit nor an inherited read"
    )


def test_a_base_that_is_no_record_is_refused(root, ac_dc):
    """A `RecordLike` has no layers to fold edits over; the error names the write that gives some.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/design/use/editing/)
    """
    from datarecord.tools.pypsa import PyPSA

    source = PyPSA.to_datarecord(ac_dc, root.store.workspace.con)
    with pytest.raises(
        TypeError, match=re.escape("WorkingRecord(ws.write(source, uri))")
    ):
        WorkingRecord(source)


# -- value forms (https://energy-models.github.io/datarecord/design/use/editing/#set) -----------------------------------------------------


@pytest.mark.parametrize(
    ("value", "labels", "match"),
    [
        ([1.0, 2.0, 3.0], ["wind1", "wind2"], "2 labels"),
        ([1.0, 2.0], None, "list keyword"),
    ],
    ids=["wrong-length", "needs-labels"],
)
def test_a_sequence_must_align_to_labels(value, labels, match):
    with pytest.raises(ValueError, match=match):
        normalise_value(value, labels)


def test_an_unnamed_series_index_over_several_open_coordinates_is_refused(staged):
    """Not inferred from the labels, so an index saying nothing must be told.

    `entity=` fixes one coordinate, but `p_max_pu` has more than one left open,
    so nothing says which the index holds.
    """
    series = pd.Series({"2030-01-01": 0.4})
    with pytest.raises(ValueError, match="naming the series' index"):
        staged.set("p_max_pu", series, entity=["Manchester Wind"])


def test_the_series_index_name_says_what_it_holds(staged):
    """A caller who named the index has said which coordinate it holds."""
    index = pd.Index([pd.Timestamp("2015-01-01")], name="snapshot")
    staged.set("p_max_pu", pd.Series([0.4], index=index), entity=["Manchester Wind"])
    assert "p_max_pu" in staged.attributes


# -- set (https://energy-models.github.io/datarecord/design/use/editing/#set) -------------------------------------------------------------


def test_a_staged_edit_is_visible_through_the_record(staged):
    """A set of pending edits is a layer, so the read resolves over it.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].df()
    mine = rows[rows["entity"] == "Manchester Wind"]
    assert set(mine["value"]) == {0.42}
    assert mine["snapshot"].isna().all(), (
        "a broadcast edit displaces the whole series: rows never overlap"
    )
    assert set(rows["entity"]) > {"Manchester Wind"}, (
        "every other name still reads the base record's rows"
    )


def test_two_lazy_reads_stay_bound_to_their_own_relations(staged, root):
    """A `Record` hands over unmaterialised frames, so two must not alias.

    The read path composes relations by replacement scan, which binds each one
    at build time. Registering them under a fixed catalog name instead would
    rebind on the second read and both frames would collapse onto the last
    one - the frames are lazy, so nothing forces the first before that happens.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/use/reading/#relations)
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    first = staged.attributes["p_max_pu"]

    staged.set("p_min_pu", 0.11, entity=["Manchester Wind"])
    second = staged.attributes["p_min_pu"]

    # Collected only now, after the second frame was built.
    got_first = first.df()
    got_second = second.df()
    assert set(got_first["attribute"]) == {"p_max_pu"}
    assert 0.42 in set(got_first["value"])
    assert set(got_second["attribute"]) == {"p_min_pu"}

    # And nothing was left behind in the catalog to leak into the next read.
    views = {
        v
        for (v,) in root.store.workspace.con.sql(
            "SELECT view_name FROM duckdb_views()"
        ).fetchall()
    }
    assert not {v for v in views if v.startswith("_")}


def test_set_rejects_an_unknown_name(staged):
    """A value for a name no layer declares would resolve to nothing.

    Validation applies to the frame form too: a typo is caught where it is typed.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    - [validation](https://energy-models.github.io/datarecord/design/use/editing/#validation)
    """
    with pytest.raises(KeyError, match="Nope"):
        staged.set("p_max_pu", pd.DataFrame([{"entity": "Nope", "value": 1.0}]))


_SNAPSHOT = "does not fit 'snapshot' of dtype datetime"
_ENTITY = "does not fit 'entity' of dtype str"


@pytest.mark.parametrize(
    ("edit", "match"),
    [
        pytest.param(
            lambda s: s.set(
                "p_max_pu",
                pd.Series({"2030-01-01": 0.4}).rename_axis("snapshot"),
                entity="Manchester Wind",
            ),
            _SNAPSHOT,
            id="series-index",
        ),
        pytest.param(
            lambda s: s.set("p_max_pu", 0.4, entity="Manchester Wind", snapshot=5),
            _SNAPSHOT,
            id="selector-keyword",
        ),
        pytest.param(
            lambda s: s.set("p_nom", 1.0, entity=[1, "Manchester Wind"]),
            _ENTITY,
            id="selector-keyword-mixed",
        ),
        pytest.param(
            lambda s: s.set("entity.ctype", {1: GEN}),
            _ENTITY,
            id="property-mapping-keys",
        ),
        pytest.param(
            lambda s: s.set("connection.role", "input", bus=5),
            "does not fit 'bus' of dtype str",
            id="multi-dim-keyword",
        ),
        pytest.param(
            lambda s: s.add("snapshot", ["2030-01-01"]),
            _SNAPSHOT,
            id="add",
        ),
        pytest.param(
            lambda s: s.add("carrier", [1, "x"]),
            "does not fit 'carrier' of dtype str",
            id="add-mixed-str",
        ),
        pytest.param(
            lambda s: s.add("entity", ["zz"], carrier=5),
            "does not fit 'carrier' of dtype str",
            id="add-a-property-referring-to-a-dim",
        ),
        pytest.param(
            lambda s: s.remove("entity", [1]),
            _ENTITY,
            id="remove",
        ),
    ],
)
def test_a_label_of_a_type_that_does_not_fit_its_dim_is_refused(staged, edit, match):
    """A label is held to its dim's dtype as a frame's column is, never parsed into it.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/use/editing/#validation)
    """
    with pytest.raises(ValueError, match=re.escape(match)):
        edit(staged)


@pytest.mark.parametrize(
    ("dim", "labels"),
    [
        pytest.param(
            "snapshot",
            pd.date_range("2031-01-01", periods=2, freq="h"),
            id="nanosecond-timestamps",
        ),
        pytest.param("period", [np.int64(2040)], id="numpy-int"),
    ],
)
def test_a_label_its_dtype_widens_is_taken(staged, dim, labels):
    """A label of a type its dtype widens, as a frame's column is, is staged."""
    staged.add(dim, labels)
    assert set(labels) <= set(staged.dims[dim].df()[dim]), "staged as given"


def test_set_accepts_a_name_staged_by_add(staged, root):
    """`add` makes the name exist, so a value for it is no longer unknown.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/use/editing/#validation)
    """
    staged.add("entity", ["NewSolar"], ctype=GEN)
    assert "NewSolar" in _entity_axis(staged).index, "it reads back before commit"
    staged.set("p_nom", 7.0, entity=["NewSolar"])

    child = root.store.commit(staged)
    assert _entity_axis(child.record).loc["NewSolar", "ctype"] == GEN
    assert _entity_values(child.record, "p_nom")["NewSolar"] == 7.0


# -- the overlay does not overlap (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule, https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits) -----------------------------


def test_a_pointwise_edit_keeps_the_rest_of_the_series(staged):
    """An edit naming a coordinate displaces that one only.

    Keying the overlay on the owner-map key alone would drop the whole series here,
    since it excludes the dims an attribute is not owned per.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    """
    base = staged.attributes["p_max_pu"].df()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    one = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one)

    rows = staged.attributes["p_max_pu"].df()
    got = rows[rows["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(got) == len(mine)
    assert got.iloc[0]["value"] == 0.123
    assert got.iloc[1:]["value"].tolist() == mine.iloc[1:]["value"].tolist()


def test_set_with_no_names_over_a_non_partial_dim_stays_one_row(store):
    """A scalar with no `entity=` leaves `entity` NULL, and a non-partial dim keeps it so.

    The read does not expand the broadcast per label when `entity` is not
    `partial`.
    """
    revision = empty_root(
        store,
        Schema(
            dimensions={"entity": Dimension(dtype="str")},
            attributes={"p_nom": AttributeSpec(dtype="float", dims={"entity"})},
            partial=frozenset(),
        ),
    ).commit()
    added = WorkingRecord(revision.record)
    added.add("entity", ["a", "b"])
    added.set("p_nom", {"a": 1.0, "b": 2.0})
    root = revision.store.commit(added)

    staged = WorkingRecord(root.record)
    staged.set("p_nom", 9.0)
    rows = root.store.commit(staged).record.attributes["p_nom"].df()
    assert rows["entity"].isna().all(), "one broadcast row, not one per label"
    assert rows["value"].tolist() == [9.0]


def test_flags_report_a_dim_a_staged_edit_introduces(staged, ac_dc):
    """A staged row's dims join the flags, unioned with the base answer.

    `flags` is the one non-`Relations` member of `Record`, so the promise that a
    read reflects pending edits has to hold for it too - and it decides which
    container a consumer puts a value in (`PyPSA.build` splits static from
    series on exactly this). `marginal_cost` starts broadcast over `snapshot`
    and varying over nothing; a per-snapshot edit must add `snapshot` to
    `varies` while leaving `broadcast` alone, since the base's NULL-snapshot
    rows are still there.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
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

    staged.set("marginal_cost", 3.0, entity=["Norway Wind"])
    mixed = staged.flags()["marginal_cost"]
    assert {"snapshot"} <= mixed.varies & mixed.broadcast, (
        "members staged at different grains report both, whatever the map's grain"
    )


def test_a_staged_edit_is_read_back_and_then_re_read(staged, root):
    """Set, read, set again, read again: the second read must see the second edit.

    That is the whole content of "the staged step is not cached", and it fails
    loudly if anyone later materialises above the frozen layers - a
    `cached_property` over the tail, or a `.create()` that does not stop where
    `frozen` does. The first read is what arms it: it builds the fold, so a
    cache introduced anywhere in it would be populated before the second `set`.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    """

    def p_nom(name):
        return _entity_values(staged, "p_nom").loc[[name]].tolist()

    staged.set("p_nom", 11.0, entity=["Manchester Wind"])
    assert p_nom("Manchester Wind") == [11.0]

    staged.set("p_nom", 22.0, entity=["Manchester Wind"])
    assert p_nom("Manchester Wind") == [22.0], "the read re-folds the staged layer"

    # A key the second edit never named, to pin that re-folding is not
    # re-resolving from scratch and dropping what the first edit staged.
    staged.set("p_nom", 33.0, entity=["Norway Wind"])
    assert p_nom("Manchester Wind") == [22.0]
    assert p_nom("Norway Wind") == [33.0]

    committed = _entity_values(root.store.commit(staged).record, "p_nom")
    assert committed["Manchester Wind"] == 22.0, "the later edit to a key commits"


def _rows(rel):
    return sorted(map(repr, rel.fetchall()))


@pytest.mark.parametrize(
    ("member", "name", "edit"),
    [
        (
            "attributes",
            "p_nom_max",
            lambda w: w.set("p_nom_max", 10.0, entity="Norway Gas"),
        ),
        ("attributes", "p_nom", lambda w: w.remove("entity", ["Norway Gas"])),
        (
            "properties",
            "entity",
            lambda w: w.set("entity.carrier", {"Norway Gas": "wind"}),
        ),
        ("dims", "entity", lambda w: w.add("entity", ["Solar"], ctype=GEN)),
        ("dims", "scenario", lambda w: w.add("scenario", ["high"])),
        (
            "dims",
            "connection",
            lambda w: w.add(
                "connection", [{"entity": "Norway Gas", "bus": "Manchester"}]
            ),
        ),
    ],
    ids=[
        "set-without-rows",
        "remove-entity",
        "set-property",
        "add-label",
        "add-to-an-empty-dim",
        "add-tuple",
    ],
)
def test_a_relation_built_before_an_edit_reads_it(staged, member, name, edit):
    """A relation of a `WorkingRecord` is planned from the schema and table names alone.

    So one built before an edit, as a view of it is, reads what one built
    after does.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    """
    rel = getattr(staged, member)[name]
    before = _rows(rel)
    edit(staged)
    after = _rows(getattr(staged, member)[name])
    assert after != before, "the edit changes the relation"
    assert _rows(rel) == after, "the relation built before the edit reads it"


@pytest.mark.parametrize(
    "materialised",
    [None, "root", "base"],
    ids=["live", "root-materialised", "base-materialised"],
)
def test_a_staged_fold_reads_as_the_fold_over_every_layer(store, materialised):
    """Edits fold onto the base's fold tables and read as every layer folded from files.

    The base is three layers deep. It has removed a label and a tuple, each in
    a layer above the one that added it, and a snapshot added and removed in
    one layer, whose staged values go with it. The edits add
    the removed label and tuple back, and each comes where it is added again.
    Writing the edits as a child folds every layer from its files, which the
    seeded fold has to match: the same labels, tuples, properties and values,
    in member order. One case materialises the root, below every removal, and
    one the base, above them, where its node cache keeps a removed key at most
    as a dead label. In every case a child over the base reads the same once
    the base is materialised.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    """
    root = _small(store)
    first = WorkingRecord(root.record)
    first.add("bus", ["Manchester", "Oslo"])
    first.add(
        "connection",
        [
            {"entity": "Manchester Wind", "bus": "Manchester"},
            {"entity": "Norway Wind", "bus": "Norway"},
            {"entity": "Norway Wind", "bus": "Oslo"},
        ],
    )
    first.remove("entity", ["Norway"])
    snapshots = [pd.Timestamp("2030-01-01"), pd.Timestamp("2030-01-02")]
    first.add("snapshot", snapshots)
    first.set("p_max_pu", 0.5, entity="Norway Wind", snapshot=snapshots)
    first.remove("snapshot", snapshots[1:])
    one = root.store.commit(first)
    second = WorkingRecord(one.record)
    second.remove("connection", [{"entity": "Norway Wind", "bus": "Norway"}])
    base = one.store.commit(second)
    if materialised:
        {"root": root, "base": base}[materialised].materialise()

    w = WorkingRecord(base.record)
    w.remove("entity", ["Manchester Wind"])
    w.add("entity", ["Solar"], ctype=GEN)
    w.add("entity", ["Norway"], ctype="Bus")
    w.remove("connection", [{"entity": "Norway Wind", "bus": "Oslo"}])
    w.add(
        "connection",
        pd.DataFrame({"entity": ["Solar", "Norway Wind"], "bus": ["Oslo", "Norway"]}),
    )
    w.set("p_nom", 5.0, entity=["Solar"])
    child = base.store.commit(w)

    def read(record):
        return (
            {d: record.dims[d].fetchall() for d in record.dims},
            {s: record.properties[s].fetchall() for s in record.properties},
            {
                a: sorted(record.attributes[a].fetchall(), key=str)
                for a in record.attributes
            },
        )

    assert w.dims["entity"].fetchall() == [
        ("Norway Wind",),
        ("Solar",),
        ("Norway",),
    ], "a staged removal goes, and a label added back comes where it is added again"
    assert [c for (c,) in w.dims["connection"].fetchall()] == [
        {"entity": "Solar", "bus": "Oslo"},
        {"entity": "Norway Wind", "bus": "Norway"},
    ], "a tuple added back comes where it is added again"
    assert w.attributes["p_max_pu"].project("snapshot").fetchall() == [
        (snapshots[0],)
    ], "the base's dead snapshot drops its value"
    folded = read(child.record)
    assert read(w) == folded, "the seeded fold reads as the fold over every layer"
    base.materialise()
    assert read(base.store.commit(w).record) == folded, (
        "materialising the base changes nothing a child of it reads"
    )


# -- ownership granularity (https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override, https://energy-models.github.io/datarecord/design/use/editing/#edits) -------------------------------------


def test_two_edits_on_one_whole_owned_series_both_land(staged, root):
    """A second `set` replaces its own fill without disturbing the first edit's.

    The case `staging-without-seq` leans on hardest: a `set` on a whole-owned
    axis stages the touched value plus the untouched coordinates as fills, and a
    second `set` at another coordinate must replace only the fill on *its* key -
    the first edit's value and its fills have to survive. Idempotence of
    `_complete_owned_whole` and replace-by-coordinate are what make it hold with
    no ordering column to rank a fill below an edit.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    """
    assert "snapshot" not in staged.schema.owned_per("p_max_pu"), (
        "owned per entity, but not per snapshot, the axis this test is about"
    )
    base = staged.attributes["p_max_pu"].df()
    mine = base[base["entity"] == "Manchester Wind"].sort_values("snapshot")
    assert len(mine) > 2, "the series needs an untouched middle to keep"

    first = mine.iloc[[0]][["entity", "snapshot"]].assign(value=0.111)
    second = mine.iloc[[1]][["entity", "snapshot"]].assign(value=0.222)
    staged.set("p_max_pu", first)
    staged.set("p_max_pu", second)

    child = root.store.commit(staged)
    rows = _layer_rows(child, "p_max_pu")
    got = rows[rows["entity"] == "Manchester Wind"].sort_values("snapshot")

    assert len(got) == len(mine), "the whole extent is carried, once per snapshot"
    assert got.iloc[0]["value"] == 0.111, "the first edit survives the second"
    assert got.iloc[1]["value"] == 0.222, "the second edit replaced its own fill"
    assert got.iloc[2:]["value"].tolist() == mine.iloc[2:]["value"].tolist(), (
        "every untouched snapshot keeps the base value"
    )
    assert set(rows["entity"]) == {"Manchester Wind"}, (
        "an axis owned whole restates the touched key's extent, not every key's"
    )


# -- add and remove (https://energy-models.github.io/datarecord/design/use/editing/#add-remove) --------------------------------------------------


def _entity_axis(record):
    """The record's resolved `properties["entity"]`, indexed by label, as pandas."""
    frame = record.properties["entity"].df()
    return frame.set_index("entity")


def _entity_values(record, attribute):
    """`attribute`'s rows naming an entity alone - every other coordinate NULL - by label."""
    rows = record.attributes[attribute].df()
    others = [c for c in rows.columns if c not in ("entity", "attribute", "value")]
    return rows[rows[others].isna().all(axis=1)].set_index("entity")["value"]


@pytest.fixture(scope="module")
def _over_units(module_store):
    """A record over `unit` and `hour`, `unit` carrying a category and a site."""
    base = empty_root(
        module_store,
        Schema(
            dimensions={
                "unit": Dimension(dtype="str"),
                "hour": Dimension(dtype="int"),
                "tech": Dimension(one_of=["plant", "node"]),
            },
            properties={
                "unit": {
                    "tech": PropertySpec(one_of="tech"),
                    "site": PropertySpec(dtype="str"),
                }
            },
            attributes={
                "output": AttributeSpec(dtype="float", dims={"unit", "hour"}),
                "demand": AttributeSpec(dtype="float", dims={"hour"}),
            },
            partial=frozenset({"unit"}),
        ),
    ).commit()
    staged = WorkingRecord(base.record)
    staged.add("hour", [0])
    staged.add(
        "unit",
        ["p1", "p2", "n1"],
        tech=["plant", "plant", "node"],
        site=["north", "south", "north"],
    )
    return WorkingRecord(module_store.commit(staged).record)


@pytest.fixture
def units(_over_units):
    yield _over_units
    _over_units.rollback()


def _unit_axis(record):
    return record.properties["unit"].df().set_index("unit")


def test_set_may_change_a_category(units):
    """A category is a patch of one column of the axis row, like any property.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    units.set("unit.tech", "node", unit=["p2"])

    axis = _unit_axis(units)
    assert axis.loc["p2", "tech"] == "node", "a category may change"
    assert axis.loc["p2", "site"] == "south", "a sibling column stays put"


@pytest.mark.parametrize(
    ("op", "args", "kwargs", "error", "match"),
    [
        (
            "set",
            ("unit.site", "east"),
            {"unit": "x"},
            KeyError,
            "names 'unit' labels \\['x'\\]",
        ),
        (
            "set",
            ("unit.tech", "mill"),
            {"unit": "p1"},
            ValueError,
            "'tech' labels \\['mill'\\]",
        ),
        (
            "add",
            ("unit", ["m"]),
            {"tech": "mill"},
            ValueError,
            "'tech' labels \\['mill'\\]",
        ),
        (
            "set",
            ("output", 1.0),
            {"tech": "mill", "hour": [0]},
            ValueError,
            "'tech' labels \\['mill'\\]",
        ),
        (
            "set",
            ("output", 1.0),
            {"tech": "plant", "unit": ["p1"], "hour": [0]},
            ValueError,
            "one way",
        ),
        (
            "set",
            ("demand", 1.0),
            {"tech": "plant", "hour": [0]},
            ValueError,
            "through no coordinate",
        ),
    ],
    ids=[
        "unknown-label",
        "unknown-category",
        "add-unknown-category",
        "keyword-unknown-category",
        "keyword-and-label",
        "keyword-reaching-no-coordinate",
    ],
)
def test_an_edit_refuses_an_unknown_label_category_or_keyword(
    units, op, args, kwargs, error, match
):
    """A category is checked against the `one_of` labels of its dim, a label against the axis.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/use/editing/#validation)
    """
    with pytest.raises(error, match=match):
        getattr(units, op)(*args, **kwargs)


def test_a_dim_with_one_of_is_the_schemas_list(units):
    """Its axis is its list, in order, which no edit changes.

    Notes
    -----
    - [a dim with one_of](https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    """
    with pytest.raises(
        ValueError, match=r"`add\('tech', ...\)` is refused: 'tech' lists its labels"
    ):
        units.add("tech", ["node"])
    assert units.dims["tech"].df()["tech"].tolist() == ["plant", "node"], (
        "the axis is the list"
    )
    assert "tech" not in units.edits.axes(), "nothing is staged for it"


def _output_units(record):
    rows = record.edits.attribute("output").df()
    return dict(zip(rows["unit"], rows["value"], strict=True))


def test_a_property_keyword_selects_the_labels_there_are_now(units):
    """`tech="plant"` writes one row per plant, and nothing of the filter.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    units.set("output", 1.0, tech="plant", hour=[0])
    assert _output_units(units) == {"p1": 1.0, "p2": 1.0}, "one row per plant"

    units.set("output", 2.0, tech=["plant", "node"], site="north", hour=[0])
    assert _output_units(units) == {"p1": 2.0, "p2": 1.0, "n1": 2.0}, (
        "keywords intersect, a list matching any of its values"
    )

    units.add("unit", ["p3"], tech="plant")
    assert "p3" not in _output_units(units), "a label added later is not reached"


def test_re_adding_a_label_replaces_its_row(staged, root):
    """One axis row per label: a later `add` is the row, whatever kind it names.

    Uniqueness needs no check of its own - the axis is keyed by the label, so
    a `remove` then an `add` under another kind is one row, the later.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    staged.remove("entity", ["Manchester Wind"])
    staged.add("entity", ["Manchester Wind"], ctype="Load")

    rows = staged.edits.axis("entity").df()
    rows = rows[rows["entity"] == "Manchester Wind"]
    assert len(rows) == 1, "one label, one row"
    assert list(rows["ctype"]) == ["Load"], "the later op wins"

    child = root.store.commit(staged)
    assert _entity_axis(child.record).loc["Manchester Wind", "ctype"] == "Load"


def test_add_fills_a_declared_column_another_add_omitted(staged):
    """An `add` omitting a declared column stages it as NULL rather than failing.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    """
    staged.add("entity", ["NewSolar"], ctype=GEN)
    staged.add("entity", ["NewWind"])

    rows = staged.edits.axis("entity").df().set_index("entity")
    assert pd.isna(rows.loc["NewWind", "ctype"]), "not carried, so NULL"
    assert rows.loc["NewSolar", "ctype"] == GEN, "unaffected by the later add"


def test_a_staged_axis_is_the_whole_axis_in_order(staged, root):
    """A label of the base removed and added again takes its base place back, and a new one is appended.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    - [axis order](https://energy-models.github.io/datarecord/design/model/dims/#axis-order)
    """
    labels = [*root.record.dims["entity"].df()["entity"], "NewSolar"]
    staged.remove("entity", ["Norway Gas"])
    staged.add("entity", ["NewSolar"], ctype=GEN)
    staged.add("entity", ["Norway Gas"], ctype=GEN)
    assert staged.edits.axis("entity").df()["entity"].tolist() == labels

    child = root.store.commit(staged)
    written = _layer_axis(child, "entity")["entity"].tolist()
    assert written == labels, "the layer restates the axis whole, in that order"
    kept = _entity_values(child.record, "p_nom")["Norway Gas"]
    assert kept == _entity_values(root.record, "p_nom")["Norway Gas"], (
        "a label removed and added again in one staging area keeps its values"
    )


def test_removing_a_label_drops_its_staged_attributes(staged, root):
    """Removing a label needs no attribute enumerated, and discards values staged for it.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    staged.set("p_nom", 99.0, entity=["Norway Gas"])
    staged.remove("entity", ["Norway Gas"])
    assert "Norway Gas" not in _entity_axis(staged).index, "it reads back at once"

    child = root.store.commit(staged)
    assert "Norway Gas" not in set(child.record.dims["entity"].df()["entity"])
    assert "Norway Gas" not in _entity_values(child.record, "p_nom")


@pytest.mark.parametrize(
    "set_value",
    [False, True],
    ids=["add-remove", "add-set-remove"],
)
def test_an_add_undone_by_a_remove_writes_nothing(staged, root, set_value):
    """A label added and removed in one staging area leaves no axis and no attribute.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    staged.add("entity", ["Solar"], ctype=GEN)
    if set_value:
        staged.set("p_nom", 5.0, entity="Solar")
    staged.remove("entity", ["Solar"])
    assert staged.edits.axes() == set(), "no axis is staged"
    assert staged.edits.attributes() == set(), "no attribute is staged"

    layer = ParquetLayer(root.store.commit(staged).manifest, root.store)
    assert layer.axis("entity") is None, "the commit writes no axis"
    assert layer.attribute("p_nom") is None, "the commit writes no attribute"


# -- connect and disconnect (https://energy-models.github.io/datarecord/design/use/editing/#add-remove, https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim) --------------------------------------


def test_remove_stages_a_multi_dim_without_the_tuple(staged, root):
    """Per `(entity, bus)`, the multi-dim's own key.

    Notes
    -----
    - [attributes over a multi-dim](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
    """
    staged.remove("connection", [{"entity": "Norwich Converter", "bus": "Norwich"}])
    before = unpacked(staged.dims["connection"])
    ports = set(before[before["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in ports, "the removal reads back before commit"
    assert "Norwich DC" in ports, "deletion is per connection, not per component"

    child = root.store.commit(staged)
    rows = unpacked(child.record.dims["connection"])
    left = set(rows[rows["entity"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in left
    assert "Norwich DC" in left, "the component's other port survives the commit"


def _efficiency(record, entity="Manchester Wind") -> dict[str, float]:
    """`efficiency` per bus for one entity, as the record resolves it."""
    resolved = broadcast(record, "efficiency", "connection")
    return {bus: v for ((e, bus),), v in resolved.items() if e == entity}


def test_a_bus_keyword_addresses_one_connection(staged, root):
    """`bus=` names one connection, which the stage packs into its struct.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
    """
    staged.set("efficiency", 0.5, entity=["Manchester Wind"], bus="Manchester")

    own = staged.edits.attribute("efficiency")
    assert "bus" not in own.columns, "the row carries the connection, not its bus"
    assert own.project("connection").distinct().fetchall() == [
        ({"entity": "Manchester Wind", "bus": "Manchester"},)
    ]
    assert _efficiency(staged)["Manchester"] == 0.5

    child = root.store.commit(staged)
    assert _efficiency(child.record)["Manchester"] == 0.5


def test_a_frame_carries_a_multi_dim_as_its_struct(staged):
    """A packed struct passes through, one key replacing the last.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    for value in (0.5, 0.25):
        staged.set(
            "efficiency",
            staged.con.sql(
                "SELECT {'entity': 'Manchester Wind', 'bus': 'Manchester'} "
                f"AS connection, {value} AS value"
            ),
        )
    assert _efficiency(staged)["Manchester"] == 0.25, "one key, replaced"


def test_set_takes_a_frame_duckdb_scans(staged):
    """An arrow frame lands through DuckDB's own scanner.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    staged.set("p_nom", pa.table({"entity": ["Manchester Wind"], "value": [42]}))
    assert _entity_values(staged, "p_nom")["Manchester Wind"] == 42.0, (
        "an integer column widens to the declared float"
    )


def test_set_takes_an_all_none_column_as_a_broadcast(staged):
    """A column of None alone is a broadcast, whatever type the frame gave it.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    staged.set("p_nom", 1.0, entity=["Manchester Wind"])
    staged.set("p_nom", pd.DataFrame({"entity": [None], "value": [2.0]}))

    rows = staged.edits.attribute("p_nom").project("entity, value").fetchall()
    assert rows == [(None, 2.0)], "the broadcast replaces the staged label it covers"


@pytest.mark.parametrize(
    ("attribute", "frame", "match"),
    [
        pytest.param(
            "p_nom",
            pd.DataFrame({"entity": ["Manchester Wind"], "value": ["high"]}),
            "column 'value' has type VARCHAR, which does not fit 'p_nom' of dtype "
            "float",
            id="mistyped",
        ),
        pytest.param(
            "p_max_pu",
            pd.DataFrame(
                {
                    "entity": ["Manchester Wind"],
                    "snapshot": pd.to_datetime(["2015-01-01"]).tz_localize("UTC"),
                    "value": [0.5],
                }
            ),
            r"\['snapshot'\] carry timestamps with a time zone",
            id="time-zone",
        ),
        pytest.param(
            "p_max_pu",
            pd.DataFrame(
                {
                    "entity": ["Manchester Wind"],
                    "Snapshot": pd.to_datetime(["2015-01-01"]),
                    "value": [0.5],
                }
            ),
            r"columns \['Snapshot'\] the schema does not declare",
            id="case-variant-coordinate",
        ),
    ],
)
def test_set_refuses_a_frame_column_that_does_not_fit_its_dtype(
    staged, attribute, frame, match
):
    """A column is cast only to a wider type of its kind; a time zone is not dropped.

    A column spelling a declared one in another case is undeclared, so it is
    refused.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    with pytest.raises(ValueError, match=match):
        staged.set(attribute, frame)


@pytest.mark.parametrize(
    "select",
    [
        "'Manchester Wind' AS entity, 'Manchester' AS bus, 0.5 AS value",
        "{'entity': 'Manchester Wind', 'bus': NULL::VARCHAR} AS connection, "
        "0.5 AS value",
    ],
    ids=["flat", "partly-null"],
)
def test_a_frame_carrying_a_multi_dim_but_as_its_struct_is_rejected(staged, select):
    """Flat coordinates, beside the struct or not, or a wrong struct, are refused.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
    """
    with pytest.raises(ValueError, match="'connection'"):
        staged.set("efficiency", staged.con.sql(f"SELECT {select}"))


def test_a_layer_touching_no_tuple_resolves_no_ancestry_to_check(
    staged, root, monkeypatch
):
    """The living-tuple check resolves the ancestry only for a layer it concerns.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """

    def unwanted(*_):
        pytest.fail("resolved the ancestry for a layer with nothing to check")

    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    monkeypatch.setattr(Coords, "resolve", classmethod(unwanted))
    root.store.commit(staged)


def test_remove_of_no_labels_stages_nothing(staged):
    staged.remove("entity", [])
    assert not staged.edits.axes(), "no axis restated, and no cascade to look up"


def test_remove_then_add_starts_without_the_parents_connections(staged, root):
    """`add` puts the entity back, not its connections.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    assert _efficiency(staged), "the base carries a value to lose"
    staged.remove("entity", ["Manchester Wind"])
    staged.add("entity", ["Manchester Wind"], ctype=GEN)

    child = root.store.commit(staged)
    assert _entity_axis(child.record).loc["Manchester Wind", "ctype"] == GEN
    assert _efficiency(child.record) == {}, "its connections are re-added by hand"


_TUPLE = {"entity": "Manchester Wind", "bus": "Norway"}


@pytest.mark.parametrize(
    ("tuples", "columns", "match"),
    [
        (
            pd.DataFrame([{"connection": {"entity": "Manchester Wind"}}]),
            {},
            r"as a struct of its coordinates \['entity', 'bus'\]",
        ),
        (
            pd.DataFrame([{"connection": _TUPLE, "entity": "Manchester Wind"}]),
            {},
            r"carrying both the column 'connection' and its coordinates \['entity'\]",
        ),
        (
            pd.DataFrame([{"entity": "Manchester Wind"}]),
            {},
            r"carries no column \['bus'\]",
        ),
        (
            [{"entity": "Manchester Wind", "bus": "Norway"}, {"entity": 1, "bus": "x"}],
            {},
            "has type INTEGER, which does not fit 'entity'",
        ),
        (
            pd.DataFrame({"entity": [None], "bus": ["Norway"]}),
            {},
            "with a coordinate NULL",
        ),
        ([{"entity": "Manchester Wind"}], {}, "maps each of its coordinates"),
        ([_TUPLE], {"p_nom": [1.0, 2.0]}, "has 2 'p_nom' values for 1 tuples"),
    ],
    ids=[
        "struct-missing-field",
        "struct-and-coordinates",
        "missing-coordinate",
        "mixed-type-coordinate-list",
        "null-coordinate",
        "partial-mapping",
        "misaligned-keyword",
    ],
)
def test_add_needs_a_label_per_coordinate(staged, tuples, columns, match):
    """A tuple is whole: a label of each coordinate, typed as the dim it draws on.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    """
    with pytest.raises(ValueError, match=match):
        staged.add("connection", tuples, **columns)


def _as_flat(pairs):
    return pd.DataFrame(pairs, columns=["entity", "bus"])


def _as_mappings(pairs):
    return [{"entity": e, "bus": b} for e, b in pairs]


@pytest.mark.parametrize("shape", [_as_flat, _as_mappings], ids=["flat", "mappings"])
def test_add_and_remove_take_tuples_in_any_shape(over_roles, shape):
    """The struct column `dims` reads, a column per coordinate, or a list of mappings.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    """
    staged = over_roles
    staged.add("entity", ["l2"])
    staged.add("connection", shape([("l2", "b0")]), role="mid")
    staged.remove("connection", shape([("l1", "b0")]))
    assert _roles(staged) == {("l1", "b1"): "out", ("l2", "b0"): "mid"}


def test_add_takes_back_what_dims_reads(roles, over_roles):
    """`dims["connection"]` is a frame `add` takes as it stands.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
    """
    staged = over_roles
    staged.remove("entity", ["l1"])
    assert _roles(staged) == {}
    staged.add("entity", ["l1"])
    staged.add("connection", roles.record.dims["connection"], role="back")
    assert _roles(staged) == {("l1", "b0"): "back", ("l1", "b1"): "back"}


def test_add_refuses_what_is_no_frame_duckdb_rescans(staged):
    """A reader is consumed by its first scan, and an edit scans a frame more than once.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    reader = pa.RecordBatchReader.from_batches(pa.schema([("entity", pa.string())]), [])
    with pytest.raises(TypeError, match="is no frame"):
        staged.add("connection", reader)


def test_a_plain_dim_refuses_a_frame(staged):
    """A frame gives a multi-dim's tuples; read as labels it would be its column names."""
    with pytest.raises(TypeError, match="a frame gives the tuples of a multi-dim"):
        staged.add("entity", pd.DataFrame({"entity": ["Norway Gas"]}))


def test_set_addresses_a_multi_dim_by_its_coordinates(staged):
    with pytest.raises(KeyError, match=r"\['connection'\] are multi-dims.*`add`"):
        staged.set("efficiency", 0.5, connection=_TUPLE)


def test_set_refuses_a_coordinate(staged):
    """A coordinate is the key, which `remove` and `add` change.

    Notes
    -----
    - [coordinates](https://energy-models.github.io/datarecord/design/model/multi-dims/#coordinates)
    """
    with pytest.raises(ValueError, match="names a coordinate of the multi-dim"):
        staged.set("connection.bus", "Norway", entity="Manchester Wind")


def test_every_declared_multi_dim_reads_its_staged_rows(store):
    """A second multi-dim is not silently dropped: the reads are keyed by dim.

    `connection` is the multi-dim every fixture has, so a read path naming it
    rather than iterating the declared ones would pass everywhere except here -
    a record declaring a `corridor` would read nothing while holding rows to
    commit.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
    """
    revision = _small(
        store,
        multi_dims={
            "connection": {"entity": "entity", "bus": "bus"},
            "corridor": {"from": "entity", "to": "entity"},
        },
    )
    staged = WorkingRecord(revision.record)

    staged.add("connection", [{"entity": "Manchester Wind", "bus": "Norway"}])
    staged.add("corridor", pd.DataFrame([{"from": "Manchester Wind", "to": "Norway"}]))

    connections = unpacked(staged.dims["connection"])
    assert "Norway" in set(
        connections[connections["entity"] == "Manchester Wind"]["bus"]
    )

    corridors = unpacked(staged.dims["corridor"], "corridor")
    assert list(zip(corridors["from"], corridors["to"], strict=True)) == [
        ("Manchester Wind", "Norway")
    ], "the second multi-dim reads its own rows, not the first's"


@pytest.fixture(scope="module")
def roles(module_store):
    """A root whose `l1` attaches to `b0` and `b1`, `role` a property of `connection`."""
    base = empty_root(
        module_store,
        Schema(
            dimensions={
                "entity": Dimension(dtype="str"),
                "bus": Dimension(dtype="str"),
                "connection": Dimension(over={"entity": "entity", "bus": "bus"}),
            },
            properties={"connection": {"role": PropertySpec(dtype="str")}},
            attributes={
                "efficiency": AttributeSpec(dtype="float", dims={"connection"})
            },
            partial=frozenset({"entity", "connection"}),
        ),
    ).commit()
    staged = WorkingRecord(base.record)
    staged.add("entity", ["l1"])
    staged.add("bus", ["b0", "b1"])
    staged.add(
        "connection",
        [{"entity": "l1", "bus": "b0"}, {"entity": "l1", "bus": "b1"}],
        role=["in", "out"],
    )
    return module_store.commit(staged)


@pytest.fixture(scope="module")
def _over_roles(roles):
    return WorkingRecord(roles.record)


@pytest.fixture
def over_roles(_over_roles):
    """A staging area over `roles`, rolled back after the test."""
    yield _over_roles
    _over_roles.rollback()


def _roles(record):
    """`(entity, bus) -> role` off the record's resolved `connection` multi-dim."""
    rows = unpacked(record.properties["connection"])
    return dict(zip(zip(rows["entity"], rows["bus"]), rows["role"]))


def test_a_multi_dim_property_is_a_column_of_its_axis_file(roles, over_roles):
    """And `set` with the multi-dim's coordinates as keywords patches the tuples they select.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    """
    store = roles.store
    on_disk = pd.read_parquet(f"{store.layer_dir(roles.id)}dims/connection.parquet")
    assert "role" in on_disk.columns

    staged = over_roles
    staged.set("connection.role", "both", entity="l1", bus="b1")
    assert _roles(staged) == {("l1", "b0"): "in", ("l1", "b1"): "both"}

    child = store.commit(staged)
    assert _roles(child.record) == {("l1", "b0"): "in", ("l1", "b1"): "both"}
    patch = pd.read_parquet(f"{store.layer_dir(child.id)}dims/connection.parquet")
    assert patch["role"].tolist() == ["in", "both"], (
        "the layer restates the multi-dim whole"
    )


def test_a_multi_dim_property_with_no_coordinate_reaches_every_tuple(over_roles):
    staged = over_roles
    staged.set("connection.role", "any")
    assert set(_roles(staged).values()) == {"any"}


def test_a_multi_dim_property_refuses_what_is_no_coordinate(over_roles):
    staged = over_roles
    with pytest.raises(ValueError, match="whose tuples its coordinates"):
        staged.set("connection.role", "x", snapshot="2020")
    with pytest.raises(KeyError, match="selects no tuple"):
        staged.set("connection.role", "x", entity="l1", bus="nowhere")


def test_add_refuses_an_attribute(over_roles):
    """An axis row carries properties and keys; an attribute is long rows of `attributes/`."""
    staged = over_roles
    with pytest.raises(ValueError, match=r"attributes \['efficiency'\].*`set"):
        staged.add("entity", ["l2"], efficiency=0.5)
    frame = pd.DataFrame([{"entity": "l1", "bus": "b0", "efficiency": 0.9}])
    with pytest.raises(ValueError, match="stage them with `set"):
        staged.add("connection", frame)


def test_add_refuses_a_case_variant_of_a_property(over_roles):
    """DuckDB matches a column by name whatever its case, so only the declared spelling is taken."""
    staged = over_roles
    frame = pd.DataFrame([{"entity": "l1", "bus": "b0", "Role": 1}])
    with pytest.raises(ValueError, match=r"columns \['Role'\] the schema does not"):
        staged.add("connection", frame)


def test_add_takes_a_frame_beside_a_scalar_property(over_roles):
    """A frame carries its own rows, so a keyword beside it is one value for all."""
    staged = over_roles
    frame = pd.DataFrame({"entity": ["l1"], "bus": ["b0"]})
    staged.add("connection", frame, role="mid")
    assert _roles(staged) == {("l1", "b0"): "mid", ("l1", "b1"): "out"}
    with pytest.raises(ValueError, match="may each be a scalar"):
        staged.add("connection", frame, role=["a"])


# -- rollback (https://energy-models.github.io/datarecord/design/use/editing/#edits) --------------------------------------------------------

_SNAPSHOTS = [pd.Timestamp("2030-01-01"), pd.Timestamp("2030-01-02")]
_CONNECTION = {"entity": "Manchester Wind", "bus": "Norway"}


@pytest.fixture(scope="module")
def _over_connected(small):
    """A child of `small` with two snapshots, a `p_max_pu` series and one connection."""
    staged = WorkingRecord(small.record)
    staged.add("snapshot", _SNAPSHOTS)
    staged.add("connection", [_CONNECTION])
    staged.set("p_max_pu", [0.4, 0.6], entity="Manchester Wind", snapshot=_SNAPSHOTS)
    staged.set("efficiency", 0.9, **_CONNECTION)
    return WorkingRecord(small.store.commit(staged).record)


@pytest.fixture
def connected(_over_connected):
    yield _over_connected
    _over_connected.rollback()


def _relations(record):
    return {
        (member, name): rel
        for member in ("dims", "properties", "attributes")
        for name, rel in getattr(record, member).items()
    }


def _read(relations):
    """Every relation's rows, in order but for an attribute's, which hold none."""
    return {
        key: (sorted if key[0] == "attributes" else list)(map(repr, rel.fetchall()))
        for key, rel in relations.items()
    }


def _reads(record, relations):
    return {
        **_read(relations),
        ("flags", None): record.flags(),
        ("edits", None): (record.edits.axes(), record.edits.attributes()),
    }


@pytest.fixture(scope="module")
def fresh_reads():
    """What a fresh `WorkingRecord` over a record's base reads, once per base."""
    held = {}

    def reads(record):
        base = record._base
        if base.revision_id not in held:
            fresh = WorkingRecord(base)
            held[base.revision_id] = _reads(fresh, _relations(fresh))
        return held[base.revision_id]

    return reads


def _then_refused(edit, refused, error):
    def run(w):
        edit(w)
        with pytest.raises(error):
            refused(w)

    return run


def _set_p_nom(w):
    w.set("p_nom", 150.0, entity="Norway Wind")


@pytest.mark.parametrize(
    ("fixture", "edit"),
    [
        ("connected", _set_p_nom),
        (
            "connected",
            lambda w: w.set(
                "p_max_pu",
                pd.Series([0.1], index=pd.Index(_SNAPSHOTS[:1], name="snapshot")),
                entity=["Norway Wind"],
            ),
        ),
        ("connected", lambda w: w.set("e_nom", 10.0, entity="Norway Wind")),
        ("connected", lambda w: w.set("efficiency", 0.5, **_CONNECTION)),
        ("connected", lambda w: w.set("entity.ctype", {"Norway Wind": "Load"})),
        ("units", lambda w: w.set("unit.site", {"p1": "east"})),
        ("units", lambda w: w.set("unit.tech", "node", unit=["p2"])),
        (
            "over_roles",
            lambda w: w.set("connection.role", "both", entity="l1", bus="b1"),
        ),
        ("connected", lambda w: w.add("entity", ["Solar"], ctype=GEN)),
        ("connected", lambda w: w.add("scenario", ["high"])),
        (
            "connected",
            lambda w: w.add("connection", [{"entity": "Norway Wind", "bus": "Norway"}]),
        ),
        (
            "over_roles",
            lambda w: w.add(
                "connection", pd.DataFrame([{"entity": "l1", "bus": "b0"}]), role="mid"
            ),
        ),
        ("connected", lambda w: w.remove("entity", ["Manchester Wind"])),
        ("connected", lambda w: w.remove("snapshot", _SNAPSHOTS[:1])),
        ("connected", lambda w: w.remove("connection", [_CONNECTION])),
        (
            "connected",
            lambda w: (
                w.add("entity", ["Solar"], ctype=GEN),
                w.set("p_nom", 5.0, entity=["Solar", "Norway Wind"]),
                w.remove("entity", ["Solar"]),
            ),
        ),
        (
            "connected",
            _then_refused(
                _set_p_nom, lambda w: w.set("p_nom", 1.0, entity=["Nope"]), KeyError
            ),
        ),
        (
            "units",
            _then_refused(
                lambda w: w.add("unit", ["p3"], tech="plant"),
                lambda w: w.set("unit.tech", "nowhere", unit=["p3"]),
                ValueError,
            ),
        ),
        (
            "over_roles",
            _then_refused(
                lambda w: w.set("connection.role", "both"),
                lambda w: w.set("connection.role", "x", entity="l1", bus="nowhere"),
                KeyError,
            ),
        ),
    ],
    ids=[
        "set-scalar",
        "set-series",
        "set-without-base-rows",
        "set-multi-dim-attribute",
        "set-category",
        "set-property",
        "set-category-of-a-partial-dim",
        "set-multi-dim-property",
        "add-label",
        "add-to-an-empty-dim",
        "add-tuple",
        "add-tuple-with-property",
        "remove-label",
        "remove-series-label",
        "remove-tuple",
        "add-set-remove",
        "refused-set",
        "refused-category",
        "refused-tuple",
    ],
)
def test_rollback_reads_as_a_fresh_working_record(request, fresh_reads, fixture, edit):
    """Every read is the fresh staging area's, so one staging area serves a module.

    Read through relations built before the edit, which `rollback()` leaves
    reading since it empties the staging tables rather than dropping them; one
    built after plans the same SQL. An edit refused after another wrote is
    rolled back with it.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    staged = request.getfixturevalue(fixture)
    built = _relations(staged)
    edit(staged)
    assert staged.edits.axes() | staged.edits.attributes(), "the edit stages a change"

    staged.rollback()
    assert _reads(staged, built) == fresh_reads(staged), "it reads as a fresh one"


def test_writing_the_edits_clears_nothing(staged, root):
    """A `WorkingRecord` still holding its edits after a write reads the same.

    Clearing is `rollback()`, called or not, so a second write of the edits
    writes them again and one after `rollback()` writes nothing.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    first = root.store.commit(staged)
    still = _entity_values(staged, "p_nom")["Manchester Wind"]
    assert still == 150.0, "still staged"

    again = root.store.commit(staged)
    assert list(_layer_rows(again, "p_nom")["entity"]) == ["Manchester Wind"]
    assert list(_layer_rows(first, "p_nom")["entity"]) == ["Manchester Wind"]

    staged.rollback()
    emptied = root.store.commit(staged)
    assert _layer_rows(emptied, "p_nom").empty


# -- commit targets (https://energy-models.github.io/datarecord/design/use/editing/#edits) --------------------------------------------------


def test_edits_are_written_only_under_their_base(small, written_directory):
    """A delta against one revision would read back wrong under another.

    An axis edit carries its base's sibling columns, so the edits staged over
    `small` describe a layer over `small` and nothing else - and the edits
    over a directory, no node in a tree, a layer over none.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    store = small.store
    other = empty_root(store, small.manifest.schema).commit()
    staged = WorkingRecord(small.record)
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])

    with pytest.raises(ValueError, match="delta against"):
        store.claim(parent=other).commit(staged)
    with pytest.raises(ValueError, match="delta against"):
        store.commit(staged.edits, parent=other)
    over_a_directory = WorkingRecord(store.workspace.at(written_directory))
    with pytest.raises(ValueError, match="delta against"):
        store.commit(over_a_directory.edits, parent=other)
    assert store.commit(staged).parent == small.id


def test_the_edits_land_in_the_child_not_the_node_branched_from(staged, root):
    """Layers are write-once, so the parent still reads its own values.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    before = _entity_values(root.record, "p_nom")["Manchester Wind"]
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    child = root.store.commit(staged)

    assert _entity_values(child.record, "p_nom")["Manchester Wind"] == 150.0
    assert _entity_values(root.record, "p_nom")["Manchester Wind"] == before
    assert list(_layer_rows(child, "p_nom")["entity"]) == ["Manchester Wind"], (
        "a patch layer is the edits alone"
    )
    assert len(_entity_values(child.record, "p_nom")) > 1, "the fold resolves the rest"


def test_an_edit_over_a_directory_base_reads_back(small):
    """A staged edit must win over a directory base, as it does over a layered one.

    The two are separate sources of one fold, and `_source_for` tells them apart
    by `layer_uuid` alone - so a base and a staging area sharing an id would
    send every staged win to the base and read the edit back as the base's
    value, with a correct-looking owner map above it. The unstaged fixtures
    cannot see that: it takes an edit that is supposed to displace something.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    base = small.store.layer(small.id)
    staged = WorkingRecord(base)
    assert staged.schema == base.schema, "an edit declares nothing of its own"

    def p_nom() -> list[float]:
        return _entity_values(staged, "p_nom").loc[["Manchester Wind"]].tolist()

    assert p_nom() == [80.0], "the base holds a value for the edit to displace"
    staged.set("p_nom", 4242.0, entity=["Manchester Wind"])
    assert p_nom() == [4242.0], (
        "the staged layer owns the key, so the read takes its value"
    )

    # The base's id is derived from its revision's rather than allocated, so
    # two readers of one layer agree on which layer they are reading.
    other = WorkingRecord(small.store.layer(small.id))
    assert other._base.revision_id == staged._base.revision_id
    assert other._layer_id != staged._layer_id, "each staging area is its own layer"


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
    assert without.revision_id == with_slash.revision_id, (
        "one directory is one layer, however its URI was spelled"
    )


def test_a_standalone_write_of_the_record_is_flattened(small, tmp_path):
    """No parent to resolve against, so the whole record is written.

    The fold's `order_key` is its answer about a frame, never a column of
    one, so no file written from the resolved frames carries it.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
    """
    staged = WorkingRecord(small.record)
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    staged.add("connection", [{"entity": "Norway Wind", "bus": "Norway"}])
    out = str(tmp_path / "flat")
    workspace = small.store.workspace
    record = workspace.write(staged, out)
    assert record.schema == staged.schema
    assert _entity_values(record, "p_nom").to_dict() == {
        "Manchester Wind": 150.0,
        "Norway Wind": 100.0,
    }, "flattened: the parent's values are written, not left to a parent"

    written = sorted(Path(out).rglob("*.parquet"))
    assert {"dims/entity.parquet", "dims/connection.parquet"} <= {
        path.relative_to(out).as_posix() for path in written
    }, "the frames the fold orders must be among those written"
    for path in written:
        columns = workspace.con.read_parquet(str(path)).columns
        assert "order_key" not in columns, f"{path.relative_to(out)} carries order_key"


def test_a_string_valued_attribute_keeps_its_declared_type(staged, root):
    """A value round-trips at the dtype its attribute declares, not a guessed one.

    Both halves have to hold at once: PyPSA's `sub_network` is a string-valued
    output, and casting it as a number would `TRY_CAST` it to NULL and lose it
    with nothing raised, while `p` must come back a number.

    Notes
    -----
    - [the shape of an edit](https://energy-models.github.io/datarecord/design/use/editing/#the-shape-of-an-edit)
    """
    staged.set("sub_network", "0", entity=["Manchester Wind"])
    rows = staged.attributes["sub_network"].df()
    assert rows["value"].tolist() == ["0"], "a string value survives the round trip"

    staged.set("p", 42.0, entity=["Manchester Wind"], bus="Manchester")
    numeric = staged.attributes["p"].df()
    assert numeric["value"].tolist() == [42.0], "a numeric value comes back a number"

    committed = root.store.commit(staged).record.attributes["sub_network"].df()
    mine = committed[committed["entity"] == "Manchester Wind"]
    assert mine["value"].tolist() == ["0"], "and a committed one reads back as text"


def test_one_call_spans_component_types(staged):
    """Names decide the type, so one edit may cross types.

    `p_nom` is declared for both `Generator` and `Link`, and each name is
    validated against its own type's spec - one call, two types, no `ctype`.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    staged.set("p_nom", [150.0, 80.0], entity=["Manchester Wind", "DC link"])
    got = _entity_values(staged, "p_nom")
    assert got["Manchester Wind"] == 150.0
    assert got["DC link"] == 80.0

    staged.set("p_nom", 150.0, entity=["Manchester Wind", "DC link"])
    got = _entity_values(staged, "p_nom")
    assert got["Manchester Wind"] == got["DC link"] == 150.0, "a scalar at every label"


def test_a_set_refused_after_it_staged_leaves_the_staging_tables_as_they_were(staged):
    """`set` stages its rows before it refuses two of them that cross.

    The refusal rolls back the edit's transaction, rows and owner tables
    alike, so the record reads as it did before the call.
    """
    staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    before = _rows(staged.attributes["p_max_pu"])
    crossing = pd.DataFrame(
        {
            "entity": ["Manchester Gas", None],
            "snapshot": [None, pd.Timestamp("2015-01-01")],
            "value": [0.1, 0.2],
        }
    )
    with pytest.raises(ValueError, match="neither is the more specific"):
        staged.set("p_max_pu", crossing)
    assert _rows(staged.attributes["p_max_pu"]) == before, (
        "the refused edit staged nothing"
    )


def test_an_edit_inside_an_open_transaction_is_refused(staged):
    """An edit opens its own transaction, which DuckDB cannot nest."""
    staged.con.begin()
    try:
        with pytest.raises(duckdb.TransactionException, match="already holds one open"):
            staged.set("p_max_pu", 0.42, entity=["Manchester Wind"])
    finally:
        staged.con.rollback()
