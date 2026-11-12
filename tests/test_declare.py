# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`WorkingRecord.declare`: widening the schema, and the frame form of `set` for a multi-dim's property.

Notes
-----
- [declare](https://energy-models.github.io/datarecord/reference/editing/#declare)
"""

import pandas as pd
import pytest

from datarecord.mutable import WorkingRecord
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from tests.fixtures import broadcast, empty_root, schema

GEN = "Generator"
STEP = Dimension(over={"period": "period", "snapshot": "snapshot"})
TUPLES = [{"period": 2030, "snapshot": 0}, {"period": 2030, "snapshot": 1}]
SCENARIO = ("scenario", Dimension(dtype="str"))
P_NOM = AttributeSpec(dtype="float", dims={"entity", "snapshot", "period"})
WIDER_P_NOM = AttributeSpec(dtype="float", dims={*P_NOM.dims, "scenario"})


@pytest.fixture(scope="module")
def base(module_store):
    """A root of two generators with a `p_nom`, declaring `snapshot` and `period` and no `scenario`."""
    root = empty_root(
        module_store, schema(dims={"snapshot": "int", "period": "int"})
    ).commit()
    with WorkingRecord(root.record) as w:
        w.add("entity", ["wind", "gas"], ctype=GEN)
        w.add("bus", ["north"])
        w.add("period", [2030])
        w.add("snapshot", [0, 1])
        w.set("p_nom", [80.0, 100.0], entity=["wind", "gas"])
        return module_store.commit(w)


@pytest.fixture
def w(base):
    with WorkingRecord(base.record) as w:
        yield w


def _label(value):
    return tuple(value.values()) if isinstance(value, dict) else value


def _labels(record, dim):
    return [_label(k) for (k,) in record.dims[dim].fetchall()]


def _column(record, subject, name):
    """Property `name` by label of `subject`, a tuple as a tuple."""
    rows = record.properties[subject].project(subject, name).fetchall()
    return {_label(k): v for k, v in rows}


def _values(record, attribute, *dims):
    return set(
        record.attributes[attribute].project(*dims, "value").distinct().fetchall()
    )


def _declared(record, name):
    """What `record`'s schema declares under `name`."""
    subject, dot, prop = name.partition(".")
    if dot:
        return record.schema.properties[subject][prop]
    return record.schema.dimensions.get(name) or record.schema.attributes[name]


def _add_steps(w):
    w.add("step", TUPLES)


def _set_segments(w):
    _add_steps(w)
    w.set("step.seg", pd.DataFrame({**_frame(TUPLES), "seg": ["a", "b"]}))


def _frame(tuples):
    return {c: [t[c] for t in tuples] for c in tuples[0]}


def _set_weights(w):
    w.add("scenario", ["low", "high"])
    w.set("weight", {"low": 0.3, "high": 0.7})


def _set_p_nom_at_high(w):
    w.add("scenario", ["low", "high"])
    w.set("p_nom", 2.0, entity="wind", scenario="high")


KINDS = [
    pytest.param(
        [SCENARIO],
        lambda w: w.add("scenario", ["low", "high"]),
        lambda r: _labels(r, "scenario"),
        ["low", "high"],
        id="dim",
    ),
    pytest.param(
        [
            ("tech", Dimension(one_of=["pv", "ccgt"])),
            ("entity.tech", PropertySpec(one_of="tech")),
        ],
        lambda w: w.set("entity.tech", "pv", entity="wind"),
        lambda r: (_labels(r, "tech"), _column(r, "entity", "tech")),
        (["pv", "ccgt"], {"wind": "pv", "gas": None}),
        id="one_of dim and a property referring to it",
    ),
    pytest.param(
        [("step", STEP)],
        _add_steps,
        lambda r: _labels(r, "step"),
        [(2030, 0), (2030, 1)],
        id="multi-dim",
    ),
    pytest.param(
        [("entity.note", PropertySpec(dtype="str"))],
        lambda w: w.set("entity.note", {"wind": "offshore"}),
        lambda r: _column(r, "entity", "note"),
        {"wind": "offshore", "gas": None},
        id="property",
    ),
    pytest.param(
        [("step", STEP), ("step.seg", PropertySpec(dtype="str"))],
        _set_segments,
        lambda r: _column(r, "step", "seg"),
        {(2030, 0): "a", (2030, 1): "b"},
        id="multi-dim property",
    ),
    pytest.param(
        [SCENARIO, ("weight", AttributeSpec(dtype="float", dims={"scenario"}))],
        _set_weights,
        lambda r: broadcast(r, "weight", "scenario"),
        {("low",): 0.3, ("high",): 0.7},
        id="attribute",
    ),
    pytest.param(
        [SCENARIO, ("p_nom", WIDER_P_NOM)],
        _set_p_nom_at_high,
        lambda r: _values(r, "p_nom", "entity", "scenario"),
        {("wind", "high", 2.0), ("wind", "low", 80.0), ("gas", None, 100.0)},
        id="attribute widened over a dim",
    ),
]


@pytest.mark.parametrize(("declarations", "edit", "read", "expected"), KINDS)
def test_a_declared_name_reads_back_before_and_after_the_write(
    w, base, declarations, edit, read, expected
):
    """`scenario` is outside `partial`, so `p_nom` at `wind` keeps its base value at `low`."""
    for name, spec in declarations:
        w.declare(name, spec)
    edit(w)
    assert read(w) == expected, "the edit reads through the working record"
    assert w.edits.schema == w.schema, "the edits are written under the widened schema"
    child = base.store.commit(w)
    assert read(child.record) == expected, "and through the child"
    for name, spec in declarations:
        assert _declared(child.record, name) == spec, "the child's manifest declares it"


UNREACHED = [
    pytest.param(SCENARIO, lambda r: _labels(r, "scenario"), [], id="dim"),
    pytest.param(("step", STEP), lambda r: _labels(r, "step"), [], id="multi-dim"),
    pytest.param(
        ("entity.note", PropertySpec(dtype="str")),
        lambda r: _column(r, "entity", "note"),
        {"wind": None, "gas": None},
        id="property",
    ),
    pytest.param(
        ("unused", AttributeSpec(dtype="int", dims={"entity"})),
        lambda r: r.attributes["unused"].fetchall(),
        [],
        id="attribute",
    ),
]


@pytest.mark.parametrize(("declaration", "read", "expected"), UNREACHED)
def test_a_declared_name_no_edit_reaches_reads_empty(
    w, base, declaration, read, expected
):
    name, spec = declaration
    w.declare(name, spec)
    assert read(w) == expected, "nothing is staged by a declaration"
    child = base.store.commit(w)
    assert read(child.record) == expected
    assert _declared(child.record, name) == spec
    files = {*child.manifest.files.dims, *child.manifest.files.attributes}
    assert name.partition(".")[0] not in files, "a declaration alone writes no file"


KEPT = [
    pytest.param(
        lambda w: w.set("p_nom", 5.0, entity="gas"),
        ("entity.note", PropertySpec(dtype="str")),
        lambda w: (_values(w, "p_nom", "entity"), _column(w, "entity", "note")),
        ({("gas", 5.0), ("wind", 80.0)}, {"wind": None, "gas": None}),
        id="property after a set on its subject's attribute",
    ),
    pytest.param(
        _add_steps,
        ("step.seg", PropertySpec(dtype="str")),
        lambda w: _column(w, "step", "seg"),
        {(2030, 0): None, (2030, 1): None},
        id="property after tuples are added",
    ),
    pytest.param(
        lambda w: w.set("p_nom", 5.0, entity="gas"),
        ("p_nom", WIDER_P_NOM),
        lambda w: _values(w, "p_nom", "entity", "scenario"),
        {("gas", None, 5.0), ("wind", None, 80.0)},
        id="attribute widened after a set",
    ),
]


@pytest.mark.parametrize(("edit", "declaration", "read", "expected"), KEPT)
def test_a_declaration_after_an_edit_keeps_the_staged_rows(
    w, edit, declaration, read, expected
):
    w.declare(*SCENARIO)
    w.declare("step", STEP)
    edit(w)
    w.declare(*declaration)
    assert read(w) == expected, "the staged rows stay, NULL on the new column"


def test_a_declaration_rebuilds_no_frozen_fold_table(w, base):
    w.attributes["p_nom"].fetchall()
    before = _fold_tables(w, base)
    w.declare(*SCENARIO)
    w.declare("entity.note", PropertySpec(dtype="str"))
    assert _labels(w, "scenario") == [], "the staged tail is planned again"
    assert _fold_tables(w, base) == before, "the base's shared tables stay as built"


def _fold_tables(w, base):
    """The base's fold tables by name and oid, which a rebuild would change."""
    rows = w.con.execute(
        "SELECT t.table_name, t.table_oid FROM duckdb_tables() t"
        " JOIN duckdb_schemas() s ON t.schema_oid = s.oid"
        " WHERE s.parent_schema = '_fold' AND s.schema_name = ?",
        [base.id.hex],
    ).fetchall()
    assert rows, "a read folds the base into shared tables"
    return set(rows)


def test_declare_widens_the_schema_and_keeps_partial(w, base):
    w.declare(*SCENARIO)
    assert "scenario" in w.schema.dimensions
    assert w.edits.schema == w.schema, "`edits` is written under the widened schema"
    assert w.schema.partial == base.record.schema.partial, "`partial` stays the base's"
    assert w.edits.axes() == set() and w.edits.attributes() == set(), (
        "nothing is staged"
    )


def test_a_redeclaration_changes_what_compatible_with_allows(w):
    w.declare("p_nom", P_NOM.model_copy(update={"default": 1.0, "unit": "MW"}))
    spec = w.schema.attributes["p_nom"]
    assert (spec.default, spec.unit) == (1.0, "MW")


@pytest.mark.parametrize(("declarations", "edit", "read", "expected"), KINDS)
def test_rollback_restores_the_base_schema(w, base, declarations, edit, read, expected):
    """A rollback after a declaration on an axis and an attribute edited before it leaves the tables as they started."""
    w.set("entity.ctype", "Load", entity="gas")
    w.set("p_nom", 5.0, entity="gas")
    for name, spec in declarations:
        w.declare(name, spec)
    edit(w)
    w.rollback()
    assert w.schema == base.record.schema
    assert w.edits.schema == base.record.schema
    assert (w.edits.axes(), w.edits.attributes()) == (set(), set()), (
        "the staging tables are as they were"
    )
    assert _column(w, "entity", "ctype") == {"wind": GEN, "gas": GEN}
    assert _values(w, "p_nom", "entity") == {("wind", 80.0), ("gas", 100.0)}
    for name, spec in declarations:
        if (
            name in base.record.schema.dimensions
            or name in base.record.schema.attributes
        ):
            continue
        with pytest.raises(KeyError, match="declares no"):
            w.set(name, 1.0)


def test_a_sibling_branch_declares_the_same_name_otherwise(base):
    children = []
    for dtype in ("str", "int"):
        with WorkingRecord(base.record) as w:
            w.declare("scenario", Dimension(dtype=dtype))
            children.append(base.store.commit(w))
    assert [c.manifest.schema.dimensions["scenario"].dtype for c in children] == [
        "str",
        "int",
    ]


REFUSED = [
    pytest.param(
        "entity.note",
        Dimension(dtype="str"),
        "is named `subject.name`",
        id="dotted dim",
    ),
    pytest.param(
        "note",
        PropertySpec(dtype="str"),
        "is named `subject.name`",
        id="plain property",
    ),
    pytest.param(
        "p_nom",
        AttributeSpec(dtype="int", dims=P_NOM.dims),
        "dtype float -> int",
        id="dtype",
    ),
    pytest.param(
        "p_nom",
        AttributeSpec(dtype="float", dims={"entity"}),
        "no longer varies over",
        id="dims narrowed",
    ),
    pytest.param("p_nom", P_NOM, "declared already", id="repeated"),
    pytest.param(
        "p_nom",
        Dimension(dtype="str"),
        "a name keeps its kind",
        id="attribute as a dim",
    ),
    pytest.param(
        "entity",
        AttributeSpec(dtype="float", dims={"entity"}),
        "a name keeps its kind",
        id="dim as an attribute",
    ),
    pytest.param("ctype", Dimension(one_of=[GEN]), "dim 'ctype'", id="one_of changed"),
    pytest.param(
        "connection",
        Dimension(over=["entity", "entity"]),
        "dim 'connection'",
        id="over changed",
    ),
    pytest.param(
        "nope.x", PropertySpec(dtype="str"), "undeclared dims", id="undeclared subject"
    ),
    pytest.param(
        "ctype.x",
        PropertySpec(dtype="str"),
        "list their labels with `one_of`",
        id="property of a one_of dim",
    ),
    pytest.param(
        "_x",
        AttributeSpec(dtype="float", dims={"entity"}),
        "may not declare these names",
        id="no column",
    ),
    pytest.param(
        "entity.x",
        PropertySpec(dtype="float", default_by={"entity.ctype": {"Nope": 1.0}}),
        "not categories",
        id="malformed default_by",
    ),
]


@pytest.mark.parametrize(("name", "spec", "match"), REFUSED)
def test_a_refused_declaration_leaves_the_schema(w, name, spec, match):
    before = w.schema
    with pytest.raises(ValueError, match=match):
        w.declare(name, spec)
    assert w.schema == before


def test_declare_refuses_a_registered_record(w):
    w.register("declared")
    with pytest.raises(ValueError, match=r"registered as \['declared'\]"):
        w.declare(*SCENARIO)
    w.workspace.unregister("declared")
    w.declare(*SCENARIO)
    assert "scenario" in w.schema.dimensions, "unregistered, it declares again"


def test_rollback_of_a_declaration_is_refused_as_declare_is(w, base):
    """A rollback that clears a declaration changes the schema, so the guards of `declare` hold."""
    w.declare(*SCENARIO)
    w.register("declared")
    with pytest.raises(
        ValueError, match=r"`rollback\(\)` is refused: this record is registered"
    ):
        w.rollback()
    w.workspace.unregister("declared")
    with WorkingRecord(w) as over:
        with pytest.raises(
            ValueError, match=r"`rollback\(\)` is refused: another `WorkingRecord`"
        ):
            w.rollback()
        assert "scenario" in over.schema.dimensions, "the record over it keeps reading"
    w.rollback()
    assert w.schema == base.record.schema


def test_declare_refuses_a_record_another_is_staged_over(base):
    with WorkingRecord(base.record) as under:
        with WorkingRecord(under) as over:
            with pytest.raises(ValueError, match="another `WorkingRecord` is staged"):
                under.declare("weight", AttributeSpec(dtype="float", dims={"entity"}))
            assert over.attributes["p_nom"].fetchall(), "the record over it reads on"
        under.declare(*SCENARIO)
        assert "scenario" in under.schema.dimensions, (
            "the one over it closed, it declares"
        )


# -- the frame form of `set` for a multi-dim's property -------------------------


@pytest.fixture
def steps(w):
    """`w` with `step` over `(period, snapshot)`, its properties `seg` and `note`, and two tuples noted."""
    w.declare("step", STEP)
    w.declare("step.seg", PropertySpec(dtype="str"))
    w.declare("step.note", PropertySpec(dtype="str"))
    w.add("step", TUPLES, note="kept")
    return w


FRAMES = [
    pytest.param(
        lambda w: pd.DataFrame({**_frame(TUPLES), "seg": ["a", "b"]}),
        id="coordinate columns",
    ),
    pytest.param(
        lambda w: w.dims["step"].project(
            "step, CASE step.snapshot WHEN 0 THEN 'a' ELSE 'b' END AS seg"
        ),
        id="struct column",
    ),
]


@pytest.mark.parametrize("frame", FRAMES)
def test_a_frame_patches_a_property_per_tuple(steps, frame):
    steps.set("step.seg", frame(steps))
    assert _column(steps, "step", "seg") == {(2030, 0): "a", (2030, 1): "b"}
    assert _column(steps, "step", "note") == {(2030, 0): "kept", (2030, 1): "kept"}, (
        "the tuples keep their other properties"
    )


def test_a_frame_patches_the_tuples_it_lists(steps):
    steps.set(
        "step.seg", pd.DataFrame({"period": [2030], "snapshot": [1], "seg": ["b"]})
    )
    assert _column(steps, "step", "seg") == {(2030, 0): None, (2030, 1): "b"}


def test_a_frame_read_off_the_axis_sets_back(steps):
    frame = steps.properties["step"].df()
    frame["seg"] = [f"s{t['snapshot']}" for t in frame["step"]]
    steps.set("step.seg", frame)
    assert _column(steps, "step", "seg") == {(2030, 0): "s0", (2030, 1): "s1"}
    assert _column(steps, "step", "note") == {(2030, 0): "kept", (2030, 1): "kept"}


FRAME_REFUSED = [
    pytest.param(
        {"period": [2030], "snapshot": [2], "seg": ["c"]},
        {},
        KeyError,
        "staged axis does not hold",
        id="a tuple the axis lacks",
    ),
    pytest.param(
        {**_frame(TUPLES), "seg": ["a", "b"], "colour": ["r", "g"]},
        {},
        ValueError,
        r"columns \['colour'\] the schema does not declare",
        id="a column the axis has no place for",
    ),
    pytest.param(
        _frame(TUPLES),
        {},
        ValueError,
        "carries no column 'seg'",
        id="the property's column missing",
    ),
    pytest.param(
        {"period": [2030, 2030], "snapshot": [0, 0], "seg": ["a", "b"]},
        {},
        ValueError,
        "lists a tuple of 'step' twice",
        id="a tuple twice",
    ),
    pytest.param(
        {**_frame(TUPLES), "seg": ["a", "b"]},
        {"period": 2030},
        ValueError,
        "takes no keyword",
        id="a coordinate keyword beside the frame",
    ),
]


@pytest.mark.parametrize(("columns", "coords", "error", "match"), FRAME_REFUSED)
def test_a_frame_is_refused(steps, columns, coords, error, match):
    with pytest.raises(error, match=match):
        steps.set("step.seg", pd.DataFrame(columns), **coords)
    assert _column(steps, "step", "seg") == {(2030, 0): None, (2030, 1): None}, (
        "a refused edit stages nothing"
    )
