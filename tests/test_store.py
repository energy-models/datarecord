# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The workspace, its stores, and the tree at the location.

Notes
-----
- [the store](https://energy-models.github.io/datarecord/design/layers/#the-store)
- [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
"""

import pickle
import threading
from concurrent.futures import ThreadPoolExecutor

import duckdb
import narwhals as nw
import pytest

from datarecord import Revision, Workspace
from datarecord.files import Absent, PreconditionFailedError
from datarecord.mutable import WorkingRecord
from tests.fixtures import (
    empty_child,
    empty_root,
    export_network,
    schema,
    write_entity_type,
    write_input,
    write_schema,
)


@pytest.fixture
def tree(store, ac_dc):
    """A root holding `ac_dc`, and a child of it."""
    root = empty_root(store)
    export_network(ac_dc, root)
    return root, empty_child(root)


def _in_threads(n, fn):
    """Run `fn(i)` in `n` threads released together, and return the results."""
    barrier = threading.Barrier(n)

    def run(i):
        barrier.wait()
        return fn(i)

    with ThreadPoolExecutor(n) as pool:
        return list(pool.map(run, range(n)))


def test_a_store_needs_a_location(workspace):
    with pytest.raises(ValueError, match="needs a location"):
        workspace.store("")


def test_one_location_is_one_store(workspace, tmp_path):
    assert workspace.store(str(tmp_path)) is workspace.store(str(tmp_path) + "/")


def test_a_local_location_is_absolute(workspace, tmp_path, monkeypatch):
    """A later `chdir` cannot move it."""
    monkeypatch.chdir(tmp_path)
    store = workspace.store("relative")
    assert store.uri == str(tmp_path / "relative")


def test_another_scheme_is_refused(workspace):
    with pytest.raises(ValueError, match="s3://"):
        workspace.store("https://example.org/record")


def test_the_tree_lives_beside_the_layers(store, tree):
    """Each revision is one file under `revisions/`, read back by a new workspace.

    Notes
    -----
    - [the tree beside the layers](https://energy-models.github.io/datarecord/design/layers/#the-tree-beside-the-layers)
    """
    root, child = tree
    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        assert {r.id for r in again.revisions()} == {root.id, child.id}
        assert again.revision(child.id).ancestry() == [root.id, child.id]
    finally:
        fresh.close()


def test_a_revision_of_another_store_is_not_this_ones(workspace, tmp_path, tree):
    root, _ = tree
    other = workspace.store(str(tmp_path / "other"))
    assert other.revisions() == []
    with pytest.raises(KeyError):
        other.revision(root.id)


def test_a_revision_file_is_written_once(store, tree):
    root, _ = tree
    with pytest.raises(PreconditionFailedError):
        store.files.write_text(store._revision_uri(root.id), "{}", expect=Absent())


def test_a_revision_crosses_a_process_boundary(tree):
    """Serialised as its store's location, read back through the process workspace.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/design/layers/#crossing-a-process-boundary)
    """
    root, child = tree
    dumped = child.model_dump_json()
    assert child.store.uri in dumped

    back = Revision.model_validate_json(dumped)
    assert back.store.workspace is Workspace.process()
    assert back.ancestry() == [root.id, child.id]
    assert list(back.record.attributes) == list(child.record.attributes)

    unpickled = pickle.loads(pickle.dumps(child.store))
    assert unpickled is Workspace.process().store(child.store.uri)


def test_threads_share_one_folded_owner_map(workspace, tree):
    """Each thread reads on its own cursor, and the fold is done once.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
    """
    _, child = tree
    record = child.record

    def read(_):
        rows = record.attributes["p_max_pu"].collect().to_native()
        return workspace.con, len(rows)

    results = _in_threads(8, read)
    cursors = {id(con) for con, _ in results}
    assert len(cursors) == 8, "one cursor per thread"
    assert len({n for _, n in results}) == 1, "every thread reads the same rows"
    tables = workspace.con.sql(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'owner_map_%'"
    ).fetchall()
    assert [t for (t,) in tables if child.id.hex in t], "the map is a shared table"


def test_two_threads_missing_the_fold_at_once_both_get_it(store, tree):
    _, child = tree

    def fold(_):
        fresh = store.revision(child.id)
        fresh._resolver = None
        return sorted(fresh.resolver.inputs.fetchall(), key=str)

    first, second = _in_threads(2, fold)
    assert first == second
    assert first


def test_two_threads_adding_one_revision_row_both_succeed(workspace, store, tree):
    _, child = tree
    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        ids = _in_threads(2, lambda _: again.revision(child.id).id)
        assert ids == [child.id, child.id]
    finally:
        fresh.close()


def test_a_record_built_in_one_thread_reads_in_another(tree):
    _, child = tree
    record = child.record
    with ThreadPoolExecutor(1) as pool:
        names = pool.submit(lambda: list(record.attributes)).result()
    assert names == list(record.attributes)


def test_a_fold_table_reads_under_a_widened_schema(store):
    """The table carries no schema in its name, so it is cast on the way out.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    narrow = {"snapshot": nw.Datetime(), "period": nw.Int64()}
    write_schema(schema(dims=narrow, partial=set()), store)
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_entity_type(layer, "Generator", [{"entity": "wind"}])
    write_input(layer, "p_max_pu", [{"entity": "wind", "value": 0.4}])
    before = revision.resolver.inputs
    assert "scenario" not in str(before.types[before.columns.index("varies")])

    write_schema(schema(dims={**narrow, "scenario": nw.String()}, partial=set()), store)
    after = store.revision(revision.id)
    assert after.resolver is not revision.resolver
    inputs = after.resolver.inputs
    assert "scenario" in str(inputs.types[inputs.columns.index("varies")]), (
        "a fold made under the narrower schema reads under the wider one"
    )


def test_a_relation_from_another_connection_is_rejected(tree):
    """Copying it over would read it on a connection nobody asked to read.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
    """
    root, _ = tree
    foreign = duckdb.connect()
    try:
        w = WorkingRecord(root.record)
        frame = foreign.sql("SELECT 'Manchester Wind' AS entity, 1.0 AS value")
        with pytest.raises(ValueError, match="another connection or thread"):
            w.add("Generator", frame)
    finally:
        foreign.close()
