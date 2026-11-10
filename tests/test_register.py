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

from datarecord import Workspace
from datarecord.duck import transaction
from datarecord.mutable import WorkingRecord
from tests.fixtures import in_threads, nested_tables, scratch_tables
from tests.test_mutable import _rows, _small, _staging

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
    """`kind -> view names` under the schema `name` of the workspace's database, from the catalog."""
    found = con.sql(
        "SELECT s.parent_schema, s.schema_name, v.view_name"
        " FROM duckdb_views() v JOIN duckdb_schemas() s ON v.schema_oid = s.oid"
        " WHERE v.database_name = 'datarecord'"
    ).fetchall()
    return {
        kind: sorted(v for parent, s, v in found if parent == name and s == kind)
        for kind in KINDS
    }


def _read_views(con, name, declared, database="datarecord"):
    """Every view of `name` in `database` as order-independent rows, keyed `(kind, view)`."""
    return {
        (kind, view): _rows(con.sql(f'FROM "{database}"."{name}"."{kind}"."{view}"'))
        for kind, views in declared.items()
        for view in views
    }


def _read_relations(record):
    """Every relation of `record`, as `record` gives it now, read as `_read_views` reads its views."""
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
            f"after edit {step} every view reads as the record's relation"
        )


def test_views_outlive_their_relations(workspace, working):
    """A view keeps what its relation reads alive, for a cursor in any thread."""
    declared = _declared(working)
    gc.collect()
    [read] = in_threads(1, lambda _: _read_views(workspace.con, "edit", declared))
    assert read == _read_relations(working)


def test_registrations_keep_a_closed_working_records_tables(workspace, working):
    """Closing a registered record drops nothing a view reads; its last unregister does."""
    con = workspace.con
    working.set("p_nom", 123.0, entity="Manchester Wind")
    working.register("again")
    declared = _declared(working)
    expected = _read_relations(working)
    gc.collect()
    held = _staging(con)
    working.close()
    assert _read_views(con, "edit", declared) == expected, (
        "a view reads on after its record closes"
    )
    workspace.unregister("edit")
    assert _read_views(con, "again", declared) == expected, (
        "and on while another registration holds the tables"
    )
    workspace.unregister("again")
    assert held, "the record staged into a schema of its own"
    assert not held & _staging(con), "the last unregister drops it"


def test_a_refused_registration_leaves_the_holder_of_its_name(
    workspace, small, working
):
    """A record registered under a taken name takes no hold, nor releases the name's."""
    con = workspace.con
    declared = _declared(working)
    expected = _read_relations(working)
    held = _staging(con)
    with WorkingRecord(small.record) as other:
        with pytest.raises(duckdb.CatalogException):
            other.register("edit")
    working.close()
    assert _read_views(con, "edit", declared) == expected, (
        "the views read on, held by the registration that took the name"
    )
    workspace.unregister("edit")
    assert not held & _staging(con), "its unregister drops the tables"


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
    assert any(s != states[0] for s in seen), "the reader saw an edit"
    assert all(s in states for s in seen), (
        "a reader saw a state no sequence of whole edits leaves"
    )


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda ws, small: small.record.register("edit"), id="register"),
        pytest.param(lambda ws, small: ws.unregister("edit"), id="unregister"),
    ],
)
def test_register_and_unregister_refuse_an_open_transaction(workspace, small, call):
    workspace.con.begin()
    try:
        with pytest.raises(duckdb.TransactionException, match="already holds one open"):
            call(workspace, small)
    finally:
        workspace.con.rollback()


@pytest.mark.parametrize("fails", [None, "block", "commit"])
def test_a_transaction_undoes_its_steps_where_it_fails(fails):
    """The rollback reaches only the database, so a block's undo steps cover
    the rest, where the commit fails as much as where the block raises."""
    db = duckdb.connect()
    db.execute("CREATE SCHEMA s")
    con, other = db.cursor(), db.cursor()
    undone: list[str] = []
    try:
        with transaction(con, "test", "https://example.org") as on_rollback:
            on_rollback(lambda: undone.append("first"))
            on_rollback(lambda: undone.append("last"))
            con.execute("CREATE TABLE s.t AS SELECT 1")
            if fails == "block":
                raise RuntimeError
            if fails == "commit":
                other.execute("DROP SCHEMA s CASCADE")
    except (RuntimeError, duckdb.TransactionException):
        assert fails is not None, "a transaction that commits raises nothing"
    expected = [] if fails is None else ["last", "first"]
    assert undone == expected, "undo steps run, last first, only where it fails"


def test_unregister_leaves_a_schema_no_record_registered(workspace):
    con = workspace.con
    con.execute("CREATE SCHEMA datarecord.other")
    con.execute("CREATE TABLE datarecord.other.t (a INTEGER)")
    with pytest.raises(duckdb.CatalogException, match="no record is registered"):
        workspace.unregister("other")
    assert con.table("datarecord.other.t").columns == ["a"], (
        "a schema no record registered stays"
    )


@pytest.mark.parametrize("name", ["memory", "datarecord", "temp"])
def test_a_record_registers_under_a_database_name(workspace, small, name):
    """The workspace's database is always named, so a name that is also a database's is no catalog."""
    con = workspace.con
    con.execute("CREATE SCHEMA memory.dims")
    con.execute("CREATE TABLE memory.dims.bus AS SELECT 'decoy' AS bus")
    small.record.register(name)
    declared = _declared(small.record)
    assert _views(con, name) == declared
    assert _read_views(con, name, declared) == _read_relations(small.record)
    workspace.unregister(name)
    assert _views(con, name) == dict.fromkeys(KINDS, [])
    assert con.table("memory.dims.bus").fetchall() == [("decoy",)], (
        "unregister drops the schema in the workspace's database alone"
    )


def test_views_read_from_a_client_attaching_the_database_under_another_name(
    tmp_path, small
):
    """A client attaching the workspace's file as `r` reads every view as its relation.

    Its own default database, and a database it calls `datarecord`, hold an
    empty table of each name the views read, so a view binding a table by a
    name the client resolves reads no rows.
    """
    ws = Workspace(str(tmp_path / "workspace.duckdb"))
    w = WorkingRecord(ws.store(small.store.uri).revision(small.id).record)
    EDITS[0](w)
    w.register("edit")
    declared = _declared(w)
    want = _read_relations(w)
    ws.close()

    client = duckdb.connect()
    client.execute(f"ATTACH '{tmp_path / 'workspace.duckdb'}' AS r")
    client.execute("ATTACH ':memory:' AS datarecord")
    tables = client.sql(
        "SELECT s.parent_schema, s.schema_name, t.table_name FROM duckdb_tables() t"
        " JOIN duckdb_schemas() s ON t.schema_oid = s.oid WHERE t.database_name = 'r'"
    ).fetchall()
    for parent, schema, table in tables:
        path = ".".join(f'"{p}"' for p in (parent, schema) if p is not None)
        for database in ("memory", "datarecord"):
            if parent is not None:
                client.execute(f'CREATE SCHEMA IF NOT EXISTS {database}."{parent}"')
            client.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{path}")
            client.execute(
                f'CREATE TABLE {database}.{path}."{table}" AS FROM r.{path}."{table}" LIMIT 0'
            )
    assert _read_views(client, "edit", declared, "r") == want, (
        "every view reads its own database's tables, whatever the client names alike"
    )
    client.close()


def test_a_workspace_on_a_callers_connection(tmp_path):
    """A server's connection keeps its default database; the workspace's cursors do the work."""
    caller = duckdb.connect(str(tmp_path / "server.duckdb"))
    caller.execute("CREATE TABLE served AS SELECT 1 AS a")
    ws = Workspace(con=caller, name="dr")
    small = _small(ws.store(str(tmp_path / "store")))

    in_threads(2, lambda i: WorkingRecord(small.record).register(f"edit_{i}"))
    assert sorted(ws.registered().project("name").fetchall()) == [
        ("edit_0",),
        ("edit_1",),
    ], "each thread registers through a cursor of the caller's connection"
    assert caller.sql("SELECT current_database()").fetchall() == [("server",)]
    assert caller.sql(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'server'"
    ).fetchall() == [("served",)], "the caller's own database holds its tables alone"
    assert _rows(caller.sql("FROM dr.edit_0.attributes.p_nom")) == _rows(
        small.record.attributes["p_nom"]
    ), "the caller reads the views by the workspace's name"

    ws.close()
    assert caller.table("served").fetchall() == [(1,)], (
        "the caller's connection stays open"
    )
    assert ("dr",) not in caller.sql(
        "SELECT database_name FROM duckdb_databases()"
    ).fetchall(), "close detaches the workspace's database"
    with pytest.raises(duckdb.ConnectionException):
        ws.con.sql("SELECT 1")
    caller.close()


def test_a_callers_tables_never_stand_in_for_datarecords_relations(
    caller_workspace, small
):
    """Edits and a commit read alike whether or not the default database holds `DECOYS`."""
    theirs = WorkingRecord(
        caller_workspace.store(small.store.uri).revision(small.id).record
    )
    ours = WorkingRecord(small.record)
    for step, edit in enumerate(EDITS[:-1]):
        edit(theirs)
        edit(ours)
        assert _read_relations(theirs) == _read_relations(ours), (
            f"after edit {step} no relation reads a caller's table"
        )
    committed = caller_workspace.store(small.store.uri).commit(theirs)
    assert _read_relations(committed.record) == _read_relations(
        small.store.commit(ours).record
    ), "the commit writes no caller's table"


def test_use_lets_a_cursor_read_by_short_names(workspace, working):
    """`USE datarecord` on a cursor of its own, so no other cursor's default changes."""
    declared = _declared(working)
    with workspace.con.cursor() as cur:
        cur.execute("USE datarecord")
        short = {
            (kind, view): _rows(cur.sql(f'FROM edit."{kind}"."{view}"'))
            for kind, views in declared.items()
            for view in views
        }
        assert short == _read_views(cur, "edit", declared), (
            "`edit.dims.bus` reads as `datarecord.edit.dims.bus`"
        )
        assert cur.sql("SELECT name, revision FROM registered").fetchall() == (
            workspace.registered().project("name, revision").fetchall()
        ), "`registered` reads as `datarecord.main.registered`"


def test_closing_and_unregistering_everything_leaves_no_staging_schema(
    workspace, small
):
    for name in ("one", "two"):
        with WorkingRecord(small.record) as w:
            w.set("p_nom", 1.0, entity="Norway Wind")
            w.register(name)
            w.register(f"{name}_again")
    with WorkingRecord(small.record):
        pass
    for (name,) in workspace.registered().project("name").fetchall():
        workspace.unregister(name)
    small.record.close()
    gc.collect()
    assert _staging(workspace.con) == set()
    assert _folds(workspace.con) == set(), "every record over a fold is closed"
    assert workspace.con.sql(
        "SELECT table_name FROM duckdb_tables()"
        " WHERE database_name = 'datarecord' AND schema_name = 'main'"
    ).fetchall() == [("registered",), ("revisions",), ("schemas",)], (
        "`main` keeps only the workspace's bookkeeping"
    )
    assert scratch_tables(workspace.con) == set(), (
        "every call dropped its scratch tables"
    )


@pytest.mark.parametrize("discard", ["close", "collect"])
def test_a_working_record_over_another_holds_its_base_staging(
    workspace, small, discard
):
    """A record staged over a `WorkingRecord` reads its base's staging tables, so holds them."""
    con = workspace.con
    base = WorkingRecord(small.record)
    base.set("p_nom", 7.0, entity="Norway Wind")
    over = WorkingRecord(base)
    over.set("p_nom", 9.0, entity="Manchester Wind")
    over.register("over")
    declared = _declared(over)
    expected = _read_relations(over)
    if discard == "close":
        base.close()
        assert _read_relations(over) == expected, "the record over it reads on"
    else:
        del base, over
        gc.collect()
    assert _read_views(con, "over", declared) == expected, "and so does its view"
    workspace.unregister("over")
    if discard == "close":
        over.close()
    gc.collect()
    assert _staging(con) == set(), "both schemas go with their last holder"


def _folds(con):
    """The fold schemas in `con`'s catalog, by revision id."""
    return set(nested_tables(con, "_fold"))


def _opened(revision):
    """`revision`'s record, after a read that folds it."""
    record = revision.record
    record.attributes["p_nom"].fetchall()
    return record


@pytest.mark.parametrize("materialised", [False, True])
@pytest.mark.parametrize("others", [0, 1])
def test_closing_a_record_drops_its_fold_once_no_other_holds_it(
    workspace, small, others, materialised
):
    kept = [_opened(small) for _ in range(others)]
    revision = small.store.revision(small.id)
    _opened(revision)
    if materialised:
        revision.materialise()
    revision.record.close()
    assert (small.id.hex in _folds(workspace.con)) == bool(kept), (
        "the fold goes with the last open record over it"
    )


@pytest.mark.parametrize(
    "record",
    [
        pytest.param(lambda small: small.store.revision(small.id).record, id="record"),
        pytest.param(
            lambda small: WorkingRecord(small.store.revision(small.id).record),
            id="working",
        ),
    ],
)
@pytest.mark.parametrize(
    "derive",
    [
        pytest.param(lambda rel: rel, id="taken"),
        pytest.param(
            lambda rel: rel.filter("value > 0").set_alias("d").aggregate("count(*)"),
            id="derived",
        ),
    ],
)
def test_a_relation_keeps_its_collected_record_and_its_tables(
    workspace, small, record, derive
):
    """A chained read collects its record before fetching, and the relation keeps what it reads."""
    rel = derive(record(small).attributes["p_nom"])
    gc.collect()
    assert rel.fetchall(), "the relation reads past its record's last name"
    del rel
    gc.collect()
    assert small.id.hex not in _folds(workspace.con), (
        "the fold goes with the record and the last relation taken from it"
    )
    assert _staging(workspace.con) == set(), "and so do the staging tables"


@pytest.mark.parametrize(
    "use",
    [
        pytest.param(lambda w: w.set("p_nom", 1.0, entity="Norway Wind"), id="set"),
        pytest.param(lambda w: w.add("bus", ["Oslo"]), id="add"),
        pytest.param(lambda w: w.edits.axes(), id="edits"),
        pytest.param(_read_relations, id="read"),
    ],
)
def test_a_working_record_works_on_past_its_closed_base(workspace, small, use):
    with small.store.revision(small.id).record as base:
        w = WorkingRecord(base)
    assert small.id.hex in _folds(workspace.con), "the working record holds the fold"
    use(w)
    w.close()
    assert small.id.hex not in _folds(workspace.con), "and drops it on close"


@pytest.mark.parametrize("discard", ["close", "collect"])
def test_a_registration_holds_its_records_fold_past_the_record(
    workspace, small, discard
):
    record = _opened(small.store.revision(small.id))
    declared, expected = _declared(record), _read_relations(record)
    record.register("plain")
    if discard == "close":
        record.close()
    del record
    gc.collect()
    assert _read_views(workspace.con, "plain", declared) == expected
    workspace.unregister("plain")
    assert small.id.hex not in _folds(workspace.con), "the fold goes on unregister"


def test_unregister_finishes_a_running_statement_and_fails_a_later_one(
    workspace, small
):
    """A statement streaming over the views reads to its end; a relation over them run again finds no schema."""
    with small.store.revision(small.id).record as record:
        record.register("live")
    reader = workspace.con.cursor()
    query = "FROM datarecord.live.attributes.p_nom, range(100000)"
    rel = reader.sql(query)
    running = reader.execute(query)
    first = running.fetchmany(10)
    workspace.unregister("live")
    assert len(first) + len(running.fetchall()) == 200_000, (
        "the running statement reads on to its end"
    )
    with pytest.raises(duckdb.CatalogException, match="live"):
        rel.fetchall()


@pytest.mark.parametrize("name", ["_fold", "_staged"])
def test_register_refuses_a_schema_of_the_workspace(small, name):
    with pytest.raises(duckdb.CatalogException, match="already exists"):
        small.record.register(name)


def test_a_reopened_workspace_holds_and_unregisters_an_earlier_registration(
    tmp_path,
):
    """`registered` survives in a file database, with the schemas it holds and the means to unregister."""
    database, location = str(tmp_path / "workspace.duckdb"), str(tmp_path / "store")
    ws = Workspace(database)
    small = _small(ws.store(location))
    with small.store.revision(small.id).record as record:
        record.register("edit")
        declared, expected = _declared(record), _read_relations(record)
    ws.close()
    reopened = Workspace(database)
    try:
        _opened(reopened.store(location).revision(small.id)).close()
        assert _read_views(reopened.con, "edit", declared) == expected, (
            "the registration holds its fold past a later record's close"
        )
        reopened.unregister("edit")
        assert reopened.registered().fetchall() == []
        assert _views(reopened.con, "edit") == dict.fromkeys(KINDS, [])
        assert small.id.hex not in _folds(reopened.con), "the fold goes on unregister"
    finally:
        reopened.close()
