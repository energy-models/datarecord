# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The workspace, its stores, and the tree at the location.

Notes
-----
- [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
- [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
"""

import json
import math
import pickle
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from datarecord import Claim, Revision, Workspace
from datarecord.mutable import WorkingRecord
from datarecord.schema.specs import Dimension
from datarecord.store import write
from datarecord.store.files import LocalFiles, PreconditionFailedError
from tests.fixtures import (
    broadcast,
    empty_child,
    empty_root,
    schema,
    widened,
    write_attribute,
    write_component,
)
from tests.test_write import _SCHEMA, _long, _Source


@pytest.fixture
def tree(store):
    """A root holding one generator with a `p_max_pu`, and a child of it."""
    layer = empty_root(store, schema())
    write_component(layer, "Generator", [{"entity": "wind"}])
    write_attribute(layer, "p_max_pu", [{"entity": "wind", "value": 0.4}])
    root = layer.commit()
    return root, empty_child(root).commit()


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
    assert store.uri == (tmp_path / "relative").as_posix()


def test_another_scheme_is_refused(workspace):
    with pytest.raises(ValueError, match="s3://"):
        workspace.store("https://example.org/record")


def test_the_tree_lives_in_the_layers(store, tree):
    """Each revision is its layer's manifest, read back by a new workspace.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """
    root, child = tree
    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        assert {r.id for r in again.revisions()} == {root.id, child.id}
        assert again.revision(child.id).ancestry() == [root.id, child.id]
    finally:
        fresh.close()


def test_a_revision_is_its_location_and_id(store, tree):
    root, child = tree
    assert child.meta == {}
    assert child == store.revision(child.id), (
        "what a revision has read is no part of it"
    )
    assert child in store.revisions()
    assert len({child, store.revision(child.id), root}) == 2


def test_a_revision_of_another_store_is_not_this_ones(workspace, tmp_path, tree):
    root, _ = tree
    other = workspace.store(str(tmp_path / "other"))
    assert other.revisions() == []
    with pytest.raises(KeyError):
        other.revision(root.id)


def _p_nom(value: float = 1.0) -> _Source:
    return _Source(_SCHEMA, attributes={"p_nom": _long(value=value)})


def _pending(store) -> list[str]:
    """The ids under `layers/` holding a claim and no manifest."""
    layers = Path(store.uri, "layers")
    return [
        d.name
        for d in layers.iterdir()
        if (d / "claim.json").exists() and not (d / "manifest.json").exists()
    ]


class _Recording(LocalFiles):
    """`LocalFiles` recording the URI of every file it creates."""

    def __init__(self):
        self.written: list[str] = []

    def create(self, uri, text):
        self.written.append(uri)
        super().create(uri, text)


def test_a_commit_writes_each_file_once(workspace, store, monkeypatch):
    """A commit without a retry writes its claim, data and manifest once each.

    Node caches are not counted.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """
    files = _Recording()
    monkeypatch.setattr(workspace, "_local", files)
    parquet = write.write_parquet

    def recorded(rel, uri, con, **kwargs):
        files.written.append(uri)
        return parquet(rel, uri, con, **kwargs)

    monkeypatch.setattr(write, "write_parquet", recorded)

    root = store.commit(_p_nom(), parent=None)
    w = WorkingRecord(root.record)
    w.set("p_nom", 2.0, entity=["steel_dri"])
    child = store.commit(w, meta={"run": "a"})
    child.materialise()
    claim = store.claim(parent=child)
    claim.commit(_p_nom(3.0), meta={"status": "done"})

    assert [u for u, n in Counter(files.written).items() if n > 1] == []
    on_disk = {
        p.as_posix()
        for p in Path(store.uri, "layers").rglob("*")
        if p.is_file() and "resolved" not in p.parts
    }
    assert on_disk == set(files.written), "every file in the store was seen written"


@pytest.mark.parametrize("committed", [False, True], ids=["pending", "committed"])
def test_a_second_claim_of_an_id_fails(store, committed):
    """A claim is create-only and kept, so an id names one revision forever.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """
    claim = store.claim(parent=None)
    if committed:
        claim.commit(_p_nom())
        with pytest.raises(ValueError, match="committed already"):
            claim.commit(_p_nom())
    uri = store.claim_uri(claim.id)
    with pytest.raises(PreconditionFailedError):
        store._read_claim(claim.id).write(store.files, uri)


def test_a_pending_layer_is_invisible_to_every_read(workspace, store):
    """A layer holding a claim and data but no manifest is no revision, but a claim.

    Its commit failed after writing its axis, so its files are there.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """
    root = store.commit(_p_nom(), parent=None)
    unheld = _Source(
        _SCHEMA,
        attributes={"p_nom": _long(entity="ghost")},
        dims={"entity": pd.DataFrame({"entity": ["steel_dri"]})},
    )
    claim = store.claim(parent=root, meta={"run": "a"})
    with pytest.raises(ValueError, match="ghost"):
        claim.commit(unheld)
    assert Path(store.axis_uri(claim.id, "entity")).exists(), "its data is written"
    assert _pending(store) == [str(claim.id)]

    fresh = Workspace()
    try:
        for reader in (store, fresh.store(store.uri)):
            assert [r.id for r in reader.revisions()] == [root.id]
            assert [(c.id, c.parent, c.meta) for c in reader.claims()] == [
                (claim.id, root.id, {"run": "a"})
            ], "listed by `claims()`, with its parent and meta"
            with pytest.raises(KeyError):
                reader.revision(claim.id)
            with pytest.raises(KeyError):
                reader.layer(claim.id)
            with pytest.raises(KeyError):
                Revision(id=claim.id, store=reader).record  # noqa: B018
            with pytest.raises(FileNotFoundError):
                reader.workspace.at(store.layer_dir(claim.id))
    finally:
        fresh.close()


@pytest.mark.parametrize(
    "by",
    [
        lambda claim: {"ids": [claim.id]},
        lambda claim: {"ids": [claim.id, claim.id]},
        lambda _: {"older_than": timedelta(0)},
    ],
    ids=["ids", "repeated-id", "age"],
)
def test_cleanup_deletes_a_pending_layer_and_no_committed_one(store, by):
    """Named by id, or claimed at least `older_than` ago, which `timedelta(0)` reaches.

    Notes
    -----
    - [cleanup](https://energy-models.github.io/datarecord/design/storage/store/#cleanup)
    """
    root = store.commit(_p_nom(), parent=None)
    claim = store.claim(parent=root)
    assert store.cleanup(**by(claim)) == [claim.id]
    assert not Path(store.layer_dir(claim.id)).exists()
    assert store.claims() == []
    assert [r.id for r in store.revisions()] == [root.id]
    assert root.record.attributes["p_nom"].df()["value"].tolist() == [1.0]


def test_cleanup_refuses_a_committed_layer_and_spares_a_young_one(store):
    """Nothing is deleted unless asked, and a committed layer never.

    Notes
    -----
    - [cleanup](https://energy-models.github.io/datarecord/design/storage/store/#cleanup)
    """
    root = store.commit(_p_nom(), parent=None)
    claim = store.claim(parent=root)
    with pytest.raises(ValueError, match=str(root.id)):
        store.cleanup(ids=[claim.id, root.id])
    assert store.cleanup(older_than=timedelta(days=14)) == [], "claimed just now"
    with pytest.raises(TypeError, match="exactly one"):
        store.cleanup()
    assert [c.id for c in store.claims()] == [claim.id], "nothing was deleted"
    assert [r.id for r in store.revisions()] == [root.id]


@pytest.mark.parametrize("when", ["before", "during"])
def test_a_cleaned_up_claim_commits_no_more(store, monkeypatch, when):
    """Refused before its data is written, or after it and before the manifest.

    A cleanup racing the write leaves the files written after it, which no
    read reaches. Each write recreates its directory, as an object store
    needs none.

    Notes
    -----
    - [cleanup](https://energy-models.github.io/datarecord/design/storage/store/#cleanup)
    """
    root = store.commit(_p_nom(), parent=None)
    claim = store.claim(parent=root)
    if when == "before":
        store.cleanup(ids=[claim.id])
    else:
        parquet = write.write_parquet

        def cleaned_up(rel, uri, con, **kwargs):
            if Path(store.claim_uri(claim.id)).exists():
                store.cleanup(ids=[claim.id])
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
            return parquet(rel, uri, con, **kwargs)

        monkeypatch.setattr(write, "write_parquet", cleaned_up)
    with pytest.raises(ValueError, match="was cleaned up"):
        claim.commit(_p_nom(2.0))
    assert not Path(store.manifest_uri(claim.id)).exists()
    assert [r.id for r in store.revisions()] == [root.id]
    assert store.claims() == []


def test_a_layer_directory_without_a_claim_is_no_claim(store):
    """A claim mid-write, or a cleanup's leftovers: neither listed nor aged.

    Notes
    -----
    - [cleanup](https://energy-models.github.io/datarecord/design/storage/store/#cleanup)
    """
    claim = store.claim(parent=None)
    stray = Path(store.layer_dir(uuid4()))
    stray.mkdir()
    (stray / ".claim.json.abc123").write_text("{}")
    assert [c.id for c in store.claims()] == [claim.id]
    assert store.cleanup(older_than=timedelta(0)) == [claim.id]
    assert stray.exists(), "only a claimed layer is aged"


@pytest.mark.parametrize(
    ("meta", "found"),
    [
        ({}, {"a", "b", "c"}),
        ({"run": "a"}, {"a"}),
        ({"n": 1}, {"a", "b"}),
        ({"run": "a", "n": 1}, {"a"}),
        ({"run": "a", "n": 2}, set()),
        ({"n": 1.0}, set()),
        ({"ok": True}, {"c"}),
    ],
    ids=["none", "one", "shared", "both", "one-off", "other-type", "bool"],
)
def test_revisions_filter_by_meta(store, meta, found):
    """A revision is listed when its `meta` holds every pair given; a pending layer never.

    Notes
    -----
    - [the layers](https://energy-models.github.io/datarecord/design/storage/store/#the-layers)
    """
    metas = {
        "a": {"run": "a", "n": 1},
        "b": {"run": "b", "n": 1},
        "c": {"run": "c", "ok": True},
    }
    names = {
        store.commit(_p_nom(), parent=None, meta=m).id: name
        for name, m in metas.items()
    }
    store.claim(parent=None, meta={"run": "a", "n": 1})
    fresh = Workspace()
    try:
        for reader in (store, fresh.store(store.uri)):
            listed = {names[r.id] for r in reader.revisions(meta=meta)}
            assert listed == found, "the same from the cache as from the manifests"
    finally:
        fresh.close()


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_a_meta_float_is_finite(store, value):
    """JSON has no NaN or infinity, so either would be read back as null.

    Refused before the claim or the manifest is written.

    Notes
    -----
    - [the layers](https://energy-models.github.io/datarecord/design/storage/store/#the-layers)
    """
    with pytest.raises(ValidationError, match="finite"):
        store.claim(parent=None, meta={"x": value})
    assert not Path(store.uri, "layers").exists(), "no claim is written"
    claim = store.claim(parent=None)
    with pytest.raises(ValidationError, match="finite"):
        claim.commit(_p_nom(), meta={"x": value})
    assert _pending(store) == [str(claim.id)], "no manifest is written"


def test_a_claim_crosses_a_process_boundary(store):
    """Serialised as its store's location and id, the rest read back from its claim file.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/design/storage/concurrency/#crossing-a-process-boundary)
    """
    root = store.commit(_p_nom(), parent=None)
    claim = store.claim(parent=root, meta={"run": "a"})
    dumped = claim.model_dump_json()
    assert json.loads(dumped) == {"id": str(claim.id), "store": store.uri}

    for back in (Claim.model_validate_json(dumped), pickle.loads(pickle.dumps(claim))):
        assert back.store is Workspace.process().store(store.uri)
        assert (back.id, back.parent, back.meta, back.created) == (
            claim.id,
            root.id,
            {"run": "a"},
            claim.created,
        )
    done = pickle.loads(pickle.dumps(claim)).commit(_p_nom(2.0), meta={"ok": True})
    assert store.revision(done.id).meta == {"run": "a", "ok": True}
    with pytest.raises(KeyError, match="No claim"):
        Claim.model_validate({"id": str(uuid4()), "store": store.uri})


@pytest.mark.parametrize(
    ("given", "outcome"),
    [
        ({"status": "optimal"}, {"run": "a", "n": 3, "status": "optimal"}),
        ({"run": "a"}, {"run": "a", "n": 3}),
        ({"run": "b"}, "changes the claimed meta \\['run'\\]"),
    ],
    ids=["adds", "repeats", "changes"],
)
def test_a_commit_may_add_to_its_claims_meta_but_not_change_it(store, given, outcome):
    """A job learns its outcome at its end, so it adds a key; a claimed key stays.

    Read back by a new workspace, each value keeps its type.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """
    claim = store.claim(parent=None, meta={"run": "a", "n": 3})
    if isinstance(outcome, str):
        with pytest.raises(ValueError, match=outcome):
            claim.commit(_p_nom(), meta=given)
        assert _pending(store) == [str(claim.id)], "the layer stays pending"
        return
    assert claim.commit(_p_nom(), meta=given).meta == outcome
    fresh = Workspace()
    try:
        again = fresh.store(store.uri).revision(claim.id).meta
        assert again == outcome
        assert type(again["n"]) is int, "the cache keeps meta's types"
    finally:
        fresh.close()


def test_a_child_narrowing_its_parents_schema_stays_pending(store):
    """Each writer checks its schema against its parent's manifest, which cannot change.

    Refused before any data is written.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    wide = widened(_SCHEMA, "extra")
    root = store.commit(_Source(wide, attributes={"p_nom": _long()}), parent=None)
    claim = store.claim(parent=root)
    with pytest.raises(ValueError, match="'extra' removed"):
        claim.commit(_p_nom())
    assert [r.id for r in store.revisions()] == [root.id]
    assert [p.name for p in Path(store.layer_dir(claim.id)).iterdir()] == ["claim.json"]
    child = store.commit(_Source(wide, attributes={"p_nom": _long()}), parent=root)
    assert child.manifest.schema == wide, "a child keeping the schema is written"


def _standalone(store, root, tmp_path):
    return store.workspace.write(root.record, str(tmp_path / "flat"))


def _layer(store, root, tmp_path):  # noqa: ARG001
    return store.layer(root.id)


def _another_stores(store, root, tmp_path):  # noqa: ARG001
    other = store.workspace.store(str(tmp_path / "other"))
    return other.commit(_p_nom(), parent=None).record


@pytest.mark.parametrize("base", [_standalone, _layer, _another_stores])
def test_a_working_record_lands_on_its_base(store, tmp_path, base):
    """`store.commit(w)` writes `w`'s edits as a child of the revision `w` was built over.

    Over anything but a revision of the store, its edits have no parent there.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
    """
    root = store.commit(_p_nom(), parent=None)
    w = WorkingRecord(root.record)
    w.set("p_nom", 2.0, entity=["steel_dri"])
    with pytest.raises(TypeError, match="takes no `parent`"):
        store.commit(w, parent=root)
    child = store.commit(w)
    assert child.parent == root.id
    assert child.record.attributes["p_nom"].df()["value"].tolist() == [2.0]
    assert set(child.manifest.files.attributes) == {"p_nom"}, "only the edits"

    elsewhere = WorkingRecord(base(store, root, tmp_path))
    with pytest.raises(ValueError, match=r"`ws\.write\(w, uri\)`"):
        store.commit(elsewhere)
    assert {r.id for r in store.revisions()} == {root.id, child.id}


def test_a_parent_of_another_store_is_refused(workspace, store, tmp_path):
    other = workspace.store(str(tmp_path / "other"))
    root = other.commit(_p_nom(), parent=None)
    with pytest.raises(ValueError, match=f"{other.uri}.*{store.uri}"):
        store.commit(_p_nom(), parent=root)
    with pytest.raises(TypeError, match="needs a `parent=`"):
        store.commit(_p_nom())  # type: ignore[call-overload]


def test_a_revision_crosses_a_process_boundary(workspace, tree):
    """Serialised as its store's location, read back through the process workspace.

    Read before serialising, so a cached record would have to travel if any
    did. The fixture's workspace is not the process one, so the ancestry can
    only come from reading the manifests again.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/design/storage/concurrency/#crossing-a-process-boundary)
    """
    root, child = tree
    names = list(child.record.attributes)
    assert workspace is not Workspace.process(), "the fixture workspace is its own"
    dumped = child.model_dump_json()
    assert child.store.uri in dumped

    for back in (
        Revision.model_validate_json(dumped),
        pickle.loads(pickle.dumps(child)),
    ):
        assert back.store.workspace is Workspace.process()
        assert back.parent == root.id
        assert back.ancestry() == [root.id, child.id], "the tree is read from files"
        assert list(back.record.attributes) == names

    unpickled = pickle.loads(pickle.dumps(child.store))
    assert unpickled is Workspace.process().store(child.store.uri)


def test_threads_share_one_folded_owner_map(workspace, tree):
    """Each thread reads on its own cursor, and the fold is done once.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """
    _, child = tree
    record = child.record

    def read(_):
        rows = record.attributes["p_max_pu"].fetchall()
        return workspace.con, len(rows)

    results = _in_threads(8, read)
    cursors = {id(con) for con, _ in results}
    assert len(cursors) == 8, "one cursor per thread"
    assert len({n for _, n in results}) == 1, "every thread reads the same rows"
    tables = workspace.con.sql(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'owner_map_%'"
    ).fetchall()
    assert [t for (t,) in tables if child.id.hex in t], "the map is a shared table"


def test_a_fold_keeps_no_table_of_its_steps(workspace, tree):
    """The steps' tables go once the shared table or the persisted map holds the fold."""
    _, child = tree
    child.record.attributes["p_max_pu"].fetchall()
    child.materialise()
    steps = workspace.con.sql(
        "SELECT table_name FROM duckdb_tables() WHERE starts_with(table_name, '_fold_')"
    ).fetchall()
    assert steps == []


def test_two_threads_missing_the_fold_at_once_both_get_it(store, tree):
    _, child = tree

    def fold(_):
        fresh = store.revision(child.id)
        return sorted(fresh.record._fold.owner_map.fetchall(), key=str)

    first, second = _in_threads(2, fold)
    assert first == second
    assert first


def test_a_revision_is_cached_only_after_its_parent(store):
    """Threads reading one tree into a fresh workspace each read the leaf's whole ancestry.

    Through `revisions()` or the leaf alone, a thread that finds the leaf
    cached finds every ancestor cached too.

    Notes
    -----
    - [the layers](https://energy-models.github.io/datarecord/design/storage/store/#the-layers)
    """
    root = store.commit(_p_nom(), parent=None)
    child = store.commit(_p_nom(), parent=root)
    leaf = store.commit(_p_nom(), parent=child)

    def read(again, i):
        if i % 2:
            again.revisions()
        return again.revision(leaf.id).ancestry()

    for _ in range(10):
        fresh = Workspace()
        try:
            again = fresh.store(store.uri)
            chains = _in_threads(8, lambda i, again=again: read(again, i))
            assert chains == [[root.id, child.id, leaf.id]] * 8
        finally:
            fresh.close()


def test_a_cached_revision_without_its_parent_fails_its_ancestry(store):
    """A cache cut short, as only a fault leaves it, fails rather than reads a short tree."""
    root = store.commit(_p_nom(), parent=None)
    child = store.commit(_p_nom(), parent=root)
    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        again._cache([child.manifest])
        with pytest.raises(RuntimeError, match=f"without its parent {root.id}"):
            again.revision(child.id).ancestry()
    finally:
        fresh.close()


def test_two_threads_adding_one_revision_row_both_succeed(workspace, store, tree):
    _, child = tree
    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        ids = _in_threads(2, lambda _: again.revision(child.id).id)
        assert ids == [child.id, child.id]
    finally:
        fresh.close()


_NARROW = {"snapshot": "datetime", "period": "int"}


def _materialised_then_widened(store, partial=frozenset()):
    """A child declaring `scenario`, over a root whose map on disk predates it."""
    layer = empty_root(store, schema(dims=_NARROW, partial=set(partial)))
    write_component(layer, "Generator", [{"entity": "wind"}])
    write_attribute(layer, "p_max_pu", [{"entity": "wind", "value": 0.4}])
    revision = layer.commit()
    before = revision.record._fold.owner_map
    assert "scenario" not in str(before.types[before.columns.index("varies")])
    revision.materialise()
    uri = store.owner_map_uri(revision.id)
    persisted = store.workspace.con.sql(f"SELECT varies FROM read_parquet('{uri}')")
    assert "scenario" not in str(persisted.types[0]), (
        "the map on disk knows nothing of `scenario`"
    )

    wide = {**_NARROW, "scenario": "str"}
    child = empty_child(revision, schema(dims=wide, partial=set(partial))).commit()
    assert "scenario" in child.record.schema.dimensions
    assert "scenario" not in revision.record.schema.dimensions, (
        "the root keeps the schema it was written under"
    )
    return child


def test_a_fold_reads_under_a_widened_schema(store):
    """A map made before a dim was declared reads under a child's wider schema.

    The map is extended to the child's schema: `p_max_pu`, over `scenario`
    there, gains the field and broadcasts along it, its rows holding NULL there.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/design/layers/resolution/#extending-the-map-to-a-wider-schema)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [Flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
    """
    after = _materialised_then_widened(store)
    rows = after.record.attributes["p_max_pu"].df()
    assert rows["value"].tolist() == [0.4], (
        "a fold made under the narrower schema reads under the wider one"
    )
    flags = after.record.flags()["p_max_pu"]
    assert "scenario" not in flags.varies
    assert "scenario" in flags.broadcast, "the row from before broadcasts along it"
    assert "snapshot" in flags.broadcast, "a field the fold had still reads"


def test_a_staged_tail_folds_onto_a_fold_from_before_a_widening(store):
    """A staged tail folds onto a materialised map extended to a schema declaring `scenario`.

    The resolved read and the flags both go through the one map.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    """
    after = _materialised_then_widened(store, partial={"period"})
    w = WorkingRecord(after.record)
    w.add("period", [2030, 2040])
    w.add("scenario", ["high"])
    w.set("p_max_pu", 0.7, entity=["wind"], period=2030, scenario="high")

    inputs = w._fold.owner_map
    assert "scenario" in str(inputs.types[inputs.columns.index("varies")])
    assert broadcast(w, "p_max_pu", "period", "scenario") == {
        (2030, "high"): 0.7,
        (2040, "high"): 0.4,
    }, (
        "the staged row and the frozen one both resolve, the frozen one for the period the staged row leaves it"
    )
    flags = w.flags()["p_max_pu"]
    assert "scenario" in flags.varies, "the staged row sets it"
    assert "scenario" in flags.broadcast, "the frozen row broadcasts along it"


def test_a_dim_listing_labels_resolves_after_a_widening(store):
    """A root's fold and axis tables read under a child's schema declaring one more dim.

    A broadcast row reaches every label of the axis table, held as its file
    held it, which neither fold table is cast for on the way out.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    dims = {**_NARROW, "scenario": Dimension(one_of=["low", "high"])}
    layer = empty_root(store, schema(dims=dims))
    write_component(layer, "Generator", [{"entity": "wind"}, {"entity": "gas"}])
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "wind", "value": 0.4},
            {"entity": "gas", "scenario": "high", "value": 0.9},
        ],
    )
    revision = layer.commit()
    expected = {("gas", "high"): 0.9, ("wind", "high"): 0.4, ("wind", "low"): 0.4}

    def resolved(revision):
        return broadcast(revision.record, "p_max_pu", "entity", "scenario")

    assert resolved(revision) == expected
    after = empty_child(revision, schema(dims={**dims, "region": "str"})).commit()
    assert resolved(after) == expected, "a child reads the root under the wider schema"
    labels = after.record.dims["scenario"].df()["scenario"].tolist()
    assert labels == ["low", "high"], "the axis is the list, in its order"


def test_a_failed_materialise_leaves_the_node_unmaterialised(store, tree, monkeypatch):
    """`owner_map.parquet` is the cache's presence, so it is written last.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    from datarecord.store import cache

    root, _ = tree

    def fail(*args, **kwargs):
        raise RuntimeError("the owner map write fails")

    monkeypatch.setattr(cache, "_checkpointed", fail)
    with pytest.raises(RuntimeError):
        root.materialise()
    assert Path(store.dim_owners_uri(root.id)).exists()
    assert not cache.materialised(root.id, store), "the rest is no cache yet"
    assert not list(Path(store.resolved_dir(root.id)).rglob("*.tmp")), (
        "no temporary file is left behind"
    )


def test_a_relation_from_another_connection_is_rejected(tree):
    """Copying it over would read it on a connection nobody asked to read.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """
    root, _ = tree
    foreign = duckdb.connect()
    try:
        w = WorkingRecord(root.record)
        frame = foreign.sql("SELECT 'wind' AS entity, 1.0 AS value")
        with pytest.raises(ValueError, match="another connection or thread"):
            w.set("p_nom", frame)
    finally:
        foreign.close()
