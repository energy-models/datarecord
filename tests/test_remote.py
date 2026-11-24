# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A store and a standalone directory on S3, against a real server.

Skipped unless `DATARECORD_TEST_S3_ENDPOINT` names an S3 endpoint honouring
conditional writes atomically - SeaweedFS 4.18 or later, which Manta deploys.
A mock would pass exactly the tests the headers exist for. To run them::

    docker run --rm --name seaweedfs -p 8333:8333 -e AWS_ACCESS_KEY_ID=datarecord \
        -e AWS_SECRET_ACCESS_KEY=datarecord chrislusf/seaweedfs:4.18 server -s3
    echo "s3.configure -user=denied -access_key=denied -secret_key=denied \
        -actions=Read,List -buckets=elsewhere -apply" | docker exec -i seaweedfs weed shell
    DATARECORD_TEST_S3_ENDPOINT=http://localhost:8333 pixi run pytest tests/test_remote.py

Each test creates its own bucket. The credentials are `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` where set, and `datarecord` otherwise, which the server
above is started with: SeaweedFS 4.18 refuses an unknown key. The key `denied`
may read and list the bucket `elsewhere` only. `AWS_PROFILE` must be unset, or
botocore reads that profile instead.

Notes
-----
- [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
- [the store](https://energy-models.github.io/datarecord/reference/store/#store)
"""

import os
import re
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import UUID, uuid4

import duckdb
import pandas as pd
import pytest

from datarecord import Store, Workspace
from datarecord.store.files import PreconditionFailedError, RemoteFiles
from tests.fixtures import relation, widened
from tests.test_write import _SCHEMA, _long, _Source

ENDPOINT = os.environ.get("DATARECORD_TEST_S3_ENDPOINT")

pytestmark = pytest.mark.skipif(
    not ENDPOINT, reason="DATARECORD_TEST_S3_ENDPOINT names no S3 server"
)


@pytest.fixture
def bucket(monkeypatch):
    """A fresh bucket on the test server, with the AWS environment pointing there."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, os.environ.get(name, "datarecord"))
    monkeypatch.setenv("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    name = f"datarecord-{uuid4().hex[:16]}"
    RemoteFiles()._s3.create_bucket(Bucket=name)
    return f"s3://{name}"


def test_a_store_writes_and_reads_its_files(workspace, bucket):
    store = workspace.store(f"{bucket}/record")
    root = store.commit(_Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None)
    child = store.commit(
        _Source(_SCHEMA, attributes={"p_nom": _long(value=2.0)}), parent=root
    )

    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        assert {r.id for r in again.revisions()} == {root.id, child.id}
        back = again.revision(child.id)
        assert back.manifest.schema == _SCHEMA
        assert back.ancestry() == [root.id, child.id]
        assert list(relation(back, "p_nom").df()["value"]) == [2.0]
    finally:
        fresh.close()


def test_a_standalone_directory_writes_and_reads_its_manifest(workspace, bucket):
    out = f"{bucket}/out"
    workspace.write(_Source(_SCHEMA, attributes={"p_nom": _long()}), out)

    elsewhere = Workspace()
    try:
        record = elsewhere.at(out)
        assert record.schema == _SCHEMA
        rows = record.attributes["p_nom"].df()
        assert rows["value"].tolist() == [1.0]
    finally:
        elsewhere.close()


def test_a_workspace_on_a_callers_connection_keeps_its_unnamed_secret(bucket):
    caller = duckdb.connect()
    caller.execute("INSTALL httpfs; LOAD httpfs;")
    caller.execute("CREATE SECRET (TYPE s3, KEY_ID 'caller', SECRET 'caller')")
    ws = Workspace(con=caller)
    ws.store(f"{bucket}/record")
    names = {n for (n,) in caller.sql("SELECT name FROM duckdb_secrets()").fetchall()}
    assert len(names) == 2 and "datarecord_s3" in names, (
        "the workspace's secret sits beside the caller's, not in its place"
    )
    ws.close()
    caller.close()


def test_a_claim_and_a_manifest_are_written_once(workspace, bucket):
    store = workspace.store(f"{bucket}/record")
    root = store.commit(_Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None)
    uri = store.claim_uri(root.id)
    with pytest.raises(PreconditionFailedError):
        store._read_claim(root.id).write(store.files, uri)
    with pytest.raises(PreconditionFailedError):
        root.manifest.write(store.files, store.manifest_uri(root.id))


def test_two_writers_widening_at_once_both_land(workspace, bucket):
    """Each checks against its parent's manifest, which cannot change: no race to settle.

    Holds only on a server checking the condition under the object's write
    lock, which SeaweedFS does from 4.18.
    """
    store = workspace.store(f"{bucket}/record")
    root = store.commit(_Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None)
    widenings = [widened(_SCHEMA, "first"), widened(_SCHEMA, "second")]
    barrier = threading.Barrier(2)

    def write(i):
        barrier.wait()
        source = _Source(widenings[i], attributes={"p_nom": _long()})
        return store.commit(source, parent=root)

    with ThreadPoolExecutor(2) as pool:
        children = list(pool.map(write, range(2)))
    assert [c.manifest.schema for c in children] == widenings, "each its own schema"
    assert root.manifest.schema == _SCHEMA, "the parent is never widened"


def test_a_write_failing_part_way_leaves_a_pending_layer_to_clean_up(workspace, bucket):
    store = workspace.store(f"{bucket}/record")
    short = _long().drop(columns=["breakpoint"])
    source = _Source(
        _SCHEMA,
        dims={"entity": pd.DataFrame({"entity": ["steel_dri"], "ctype": ["Process"]})},
        attributes={"p_nom": short},
    )
    with pytest.raises(ValueError, match="missing long-schema columns"):
        store.commit(source, parent=None)

    (pending,) = store.files.list(f"{store.uri}/layers/")
    assert store.files.exists(store.claim_uri(pending)), "the layer is claimed"
    assert not store.files.exists(store.manifest_uri(pending)), "and pending"
    assert store.revisions() == [], "so no revision reaches it"
    assert [str(c.id) for c in store.claims()] == [pending]

    assert store.cleanup(older_than=timedelta(0)) == [UUID(pending)]
    assert store.files.list(f"{store.uri}/layers/") == [], "every key under it went"


def test_a_standalone_write_to_a_non_empty_uri_is_refused(workspace, bucket):
    out = f"{bucket}/out"
    files = workspace.files(out)
    files.create(f"{out}/stray.txt", "x")

    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(_Source(_SCHEMA, attributes={"p_nom": _long()}), out)
    assert files.list(out) == ["stray.txt"], "refused before anything was written"


@pytest.mark.parametrize(
    "call",
    [
        lambda store, ids: store.revision(ids[0]),
        lambda store, ids: store.revisions(),
        lambda store, ids: store.claims(),
        lambda store, ids: store.cleanup(ids=ids[1:]),
        lambda store, ids: store.cleanup(older_than=timedelta(0)),
    ],
    ids=["revision", "revisions", "claims", "cleanup-ids", "cleanup-older"],
)
def test_a_denied_read_raises_and_deletes_nothing(
    workspace, bucket, monkeypatch, call: Callable[[Store, list[UUID]], object]
):
    """A token scoped to another bucket meets a 403, never what looks like absent data."""
    store = workspace.store(f"{bucket}/record")
    root = store.commit(_Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None)
    pending = store.claim(parent=root)
    layers = sorted(store.files.list(f"{store.uri}/layers/"))

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "denied")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "denied")
    monkeypatch.setattr("boto3.DEFAULT_SESSION", None)
    elsewhere = Workspace()
    try:
        denied = re.escape(store.uri) + ".*AccessDenied"
        with pytest.raises(PermissionError, match=denied):
            call(elsewhere.store(store.uri), [root.id, pending.id])
    finally:
        elsewhere.close()

    assert sorted(store.files.list(f"{store.uri}/layers/")) == layers
    assert store.files.exists(store.manifest_uri(root.id)), "the revision stays"
    assert store.files.exists(store.claim_uri(pending.id)), "and so does the claim"
