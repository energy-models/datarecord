# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`Record.register`: a record's relations as views in a DuckDB schema.

Notes
-----
- [registered views](https://energy-models.github.io/datarecord/design/storage/views/)
"""

import gc
import threading
from uuid import uuid4

import duckdb
import pytest

from datarecord.mutable import WorkingRecord
from tests.fixtures import in_threads
from tests.test_mutable import _rows, _small

KINDS = ("dims", "properties", "attributes")

EDITS = [
    lambda w: w.set("p_nom", 123.0, entity="Manchester Wind"),
    lambda w: w.set("entity.ctype", {"Manchester Wind": "Process"}),
    lambda w: w.add("entity", ["Solar"], ctype="Generator"),
    lambda w: w.add("connection", [{"entity": "Solar", "bus": "Norway"}]),
    lambda w: w.remove("connection", [{"entity": "Solar", "bus": "Norway"}]),
    lambda w: w.remove("entity", ["Solar"]),
    lambda w: w.add("connection", [{"entity": "Manchester Wind", "bus": "Norway"}]),
    lambda w: w.remove("entity", ["Manchester Wind"]),
    lambda w: w.rollback(),
]
"""One edit script over `_small`: attribute, property, plain dim, multi-dim, rollback.

The last removal takes a label with its `connection` tuple.
"""


@pytest.fixture
def small(store):
    return _small(store)


@pytest.fixture
def working(small):
    """A fresh `WorkingRecord` over `small`, registered as `edit`."""
    w = WorkingRecord(small.record)
    w.register("edit")
    return w


def _declared(record):
    """`kind -> names` the record declares, so a view is read without its relation."""
    return {kind: sorted(getattr(record, kind)) for kind in KINDS}


def _views(con, name):
    """`kind -> view names` under the schema `name`, from the catalog."""
    found = con.sql(
        "SELECT s.parent_schema, s.schema_name, v.view_name"
        " FROM duckdb_views() v JOIN duckdb_schemas() s ON v.schema_oid = s.oid"
    ).fetchall()
    return {
        kind: sorted(v for parent, s, v in found if parent == name and s == kind)
        for kind in KINDS
    }


def _view(con, name, kind, view):
    return con.sql(f'FROM "{name}"."{kind}"."{view}"')


def _read_views(con, name, declared):
    """Every view of `name` as order-independent rows, keyed `(kind, view)`."""
    return {
        (kind, view): _rows(_view(con, name, kind, view))
        for kind, views in declared.items()
        for view in views
    }


def _read_relations(record):
    """Every relation of `record`, built afresh, as `_read_views` reads its views."""
    return {
        (kind, view): _rows(getattr(record, kind)[view])
        for kind in KINDS
        for view in getattr(record, kind)
    }


def test_a_record_registers_a_view_per_relation(ac_dc_root):
    """Registered and unregistered in the session's workspace, under a fresh name."""
    name = f"reg_{uuid4().hex[:8]}"
    ws = ac_dc_root.store.workspace
    record = ac_dc_root.record
    record.register(name)
    con = ws.con
    declared = _declared(record)

    assert _views(con, name) == declared, (
        "one view per declared dim, property subject and attribute"
    )
    assert _read_views(con, name, declared) == _read_relations(record), (
        "every view reads as its relation"
    )
    assert ws.registered().filter(f"name = '{name}'").project(
        "revision, working"
    ).fetchall() == [(ac_dc_root.id, False)]
    with pytest.raises(duckdb.CatalogException):
        record.register(name)

    ws.unregister(name)
    assert _views(con, name) == dict.fromkeys(KINDS, []), "unregister drops the views"
    assert not ws.registered().filter(f"name = '{name}'").fetchall(), (
        "unregister deletes the row"
    )


def test_views_on_a_working_record_follow_its_edits(workspace, small, working):
    con = workspace.con
    assert workspace.registered().project("name, revision, working").fetchall() == [
        ("edit", small.id, True)
    ]
    declared = _declared(working)
    assert _read_views(con, "edit", declared) == _read_relations(working)
    for step, edit in enumerate(EDITS):
        edit(working)
        assert _read_views(con, "edit", declared) == _read_relations(working), (
            f"after edit {step} every view reads as its relation built afresh"
        )


def test_views_outlive_their_relations(workspace, working):
    """A view keeps what its relation reads alive, for a cursor in any thread."""
    declared = _declared(working)
    gc.collect()
    [read] = in_threads(1, lambda _: _read_views(workspace.con, "edit", declared))
    assert read == _read_relations(working)


def test_a_reader_sees_only_whole_edits(workspace, working):
    """A reader in its own transactions sees each edit whole or not at all."""
    declared = _declared(working)
    states = [_read_views(workspace.con, "edit", declared)]
    seen = []
    read = threading.Event()
    done = threading.Event()

    def reader():
        con = workspace.con
        while not done.is_set():
            con.begin()
            seen.append(_read_views(con, "edit", declared))
            con.commit()
            read.set()

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        for edit in EDITS:
            assert read.wait(120), "the reader reads"
            read.clear()
            edit(working)
            states.append(_read_views(workspace.con, "edit", declared))
        assert read.wait(120), "the reader reads"
    finally:
        done.set()
        thread.join()

    assert len(seen) > len(EDITS), "the reader read between every two edits"
    assert all(s in states for s in seen), (
        "a reader saw a state no sequence of whole edits leaves"
    )


def test_register_refuses_an_open_transaction(workspace, small):
    workspace.con.begin()
    with pytest.raises(duckdb.TransactionException):
        small.record.register("edit")
