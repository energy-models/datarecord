# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A store and a standalone directory on S3, against a real server.

Skipped unless `DATARECORD_TEST_S3_ENDPOINT` names an S3 endpoint honouring
conditional writes atomically - SeaweedFS 4.52 or later, which Manta deploys.
A mock would pass exactly the tests the headers exist for. To run them::

    docker run -d --rm --name seaweedfs -p 8333:8333 -e AWS_ACCESS_KEY_ID=datarecord \
        -e AWS_SECRET_ACCESS_KEY=datarecord chrislusf/seaweedfs:4.52 server -s3 \
        -master.volumeSizeLimitMB=64 -volume.max=1000
    echo "s3.configure -user=denied -access_key=denied -secret_key=denied \
        -actions=Read,List -buckets=elsewhere -apply
    s3.configure -user=reader -access_key=reader -secret_key=reader \
        -actions=Read,List -apply
    s3.configure -user=layer -access_key=layer -secret_key=layer -apply
    s3.configure -user=sibling -access_key=sibling -secret_key=sibling -apply" \
        | docker exec -i seaweedfs weed shell
    DATARECORD_TEST_S3_ENDPOINT=http://localhost:8333 pixi run pytest tests/test_remote.py

Each test creates its own bucket, for which SeaweedFS grows volumes of its
own: the volume flags keep them small and plentiful. The credentials are
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` where set, and `datarecord`
otherwise, which the server above is started with: SeaweedFS 4.52 refuses an
unknown key. The key `denied` may read and list the bucket `elsewhere` only,
the key `reader` may read and list every bucket, and the keys `layer` and
`sibling` may do nothing but what a test's bucket policy grants them.

Notes
-----
- [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
- [the store](https://energy-models.github.io/datarecord/reference/store/#store)
"""

import json
import os
import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import timedelta
from uuid import UUID, uuid4

import duckdb
import pandas as pd
import pytest

from datarecord import Claim, Revision, S3Credentials, Store, Workspace
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


@pytest.fixture
def credentials(bucket, monkeypatch) -> S3Credentials:
    """The test server's key, injected, while the AWS environment holds the key `denied`."""
    injected = S3Credentials(
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint=ENDPOINT,
        region=os.environ["AWS_REGION"],
        url_style="path",
    )
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "denied")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "denied")
    return injected


def _parquet(store: Store, id: UUID) -> duckdb.DuckDBPyRelation:
    """Every parquet file of layer `id`, read by DuckDB with the workspace's secret."""
    return store.workspace.con.read_parquet(
        f"{store.layer_dir(id)}**/*.parquet", union_by_name=True
    )


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


def test_a_workspace_with_injected_credentials_writes_and_reads(bucket, credentials):
    """Neither boto3 nor DuckDB falls back to the environment's key, which is denied here."""
    ws = Workspace(credentials=credentials)
    try:
        store = ws.store(f"{bucket}/record")
        root = store.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None
        )
        assert store.revisions() == [root]
        assert list(relation(root, "p_nom").df()["value"]) == [1.0]
    finally:
        ws.close()


@pytest.mark.parametrize(
    "denied",
    [lambda injected: replace(injected, key_id="denied", secret="denied"), None],
    ids=["injected", "environment"],
)
def test_replaced_credentials_take_effect_on_a_live_workspace(
    bucket,
    credentials,
    denied: Callable[[S3Credentials], S3Credentials] | None,
):
    """Both boto3 and DuckDB's secret switch to the `denied` key, and back.

    `None` falls back to the AWS environment, which holds the key `denied`.
    """
    ws = Workspace(credentials=credentials)
    try:
        store = ws.store(f"{bucket}/record")
        root = store.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None
        )
        record = root.record
        rows = len(_parquet(store, root.id).df())

        ws.set_credentials(None if denied is None else denied(credentials))
        with pytest.raises(PermissionError, match="AccessDenied"):
            store.revisions()
        with pytest.raises(duckdb.HTTPException, match="403"):
            _parquet(store, root.id).df()

        ws.set_credentials(credentials)
        assert store.revisions() == [root]
        assert len(_parquet(store, root.id).df()) == rows
        assert record.attributes["p_nom"].df()["value"].tolist() == [1.0], (
            "a record opened before the swaps reads on"
        )
    finally:
        ws.close()


@dataclass
class _Scoped:
    store: Store
    root: Revision
    claim: Claim
    other: Claim
    key: S3Credentials
    other_key: S3Credentials


def _allow_only_their_layers(
    store: Store,
    admin: S3Credentials,
    layers: dict[str, UUID],
    denied: tuple[str, ...] = (),
    *,
    public: bool = False,
) -> None:
    """Grant each key in `layers`, by bucket policy, the S3 actions a commit takes under its layer except `denied`, and none elsewhere.

    `public` grants anyone, without a key, the reads of the whole bucket.

    Notes
    -----
    - [a commit with a key for its layer](https://energy-models.github.io/datarecord/reference/store/#a-commit-with-a-key-for-its-layer)
    """
    bucket, _, key = store.uri.removeprefix("s3://").partition("/")
    statements = []
    for user, id in layers.items():
        prefix = f"{key}/layers/{id}/"
        principal = {"AWS": f"arn:aws:iam::000000000000:user/{user}"}
        statements += [
            {
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"],
                "Resource": f"arn:aws:s3:::{bucket}/{prefix}*",
            },
            {
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket}",
                "Condition": {"StringLike": {"s3:prefix": f"{prefix}*"}},
            },
        ]
        if denied:
            statements.append(
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": list(denied),
                    "Resource": f"arn:aws:s3:::{bucket}/{prefix}*",
                }
            )
    if public:
        statements += [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket}/*",
            },
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket}",
            },
        ]
    policy = json.dumps({"Version": "2012-10-17", "Statement": statements})
    RemoteFiles(admin)._s3.put_bucket_policy(Bucket=bucket, Policy=policy)


@pytest.fixture
def scoped(bucket, credentials) -> Iterator[_Scoped]:
    """A root, two claims under it, and the keys `layer` and `sibling`, each for one claim's layer alone.

    Claimed with the server's key, then the workspace reads with the key
    `reader`, which may not write.
    """
    ws = Workspace(credentials=credentials)
    try:
        store = ws.store(f"{bucket}/record")
        root = store.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None
        )
        claim, other = store.claim(parent=root), store.claim(parent=root)
        layers = {"layer": claim.id, "sibling": other.id}
        _allow_only_their_layers(store, credentials, layers)
        ws.set_credentials(replace(credentials, key_id="reader", secret="reader"))
        key, other_key = (replace(credentials, key_id=u, secret=u) for u in layers)
        yield _Scoped(store, root, claim, other, key, other_key)
    finally:
        ws.close()


class _Overlapping(_Source):
    """A `_Source` whose every read of its attributes waits at `barrier`, so two commits write their layers in step."""

    def __init__(self, barrier: threading.Barrier, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._barrier = barrier

    @property
    def attributes(self):
        self._barrier.wait(timeout=30)
        return super().attributes


def _secrets(store: Store) -> set[str]:
    return {
        n
        for (n,) in store.workspace.con.sql(
            "SELECT name FROM duckdb_secrets()"
        ).fetchall()
    }


def test_a_commit_with_a_key_for_its_layer_writes_that_layer(scoped: _Scoped):
    """The layer's key writes the layer, and the workspace's reads the root, which the layer's may not."""
    child = scoped.claim.commit(
        _Source(_SCHEMA, attributes={"p_nom": _long(value=2.0)}), credentials=scoped.key
    )
    assert child in scoped.store.revisions()
    assert child.ancestry() == [scoped.root.id, child.id]
    assert list(relation(child, "p_nom").df()["value"]) == [2.0]
    assert _secrets(scoped.store) == {"datarecord_s3"}, "the layer's secret is gone"

    with pytest.raises(PermissionError, match="AccessDenied"):
        RemoteFiles(scoped.key).create(
            f"{scoped.store.layer_dir(scoped.other.id)}stray.txt", "x"
        )


@pytest.mark.parametrize(
    ("commit", "denied"),
    [
        (lambda s, source: s.other.commit(source, credentials=s.key), PermissionError),
        (lambda s, source: s.claim.commit(source), duckdb.HTTPException),
    ],
    ids=["another-layer", "workspace-key"],
)
def test_a_commit_without_a_key_for_its_layer_is_denied(
    scoped: _Scoped,
    commit: Callable[[_Scoped, _Source], Revision],
    denied: type[Exception],
):
    """Neither the key for another layer nor the workspace's read-only key writes a layer."""
    with pytest.raises(denied):
        commit(scoped, _Source(_SCHEMA, attributes={"p_nom": _long(value=2.0)}))
    assert scoped.store.revisions() == [scoped.root], "no layer committed"
    assert _secrets(scoped.store) == {"datarecord_s3"}, "the layer's secret is gone"


def _commit_at_once(
    scoped: _Scoped, commits: list[tuple[Claim, S3Credentials, float]]
) -> tuple[list[Revision | BaseException], list[set[str]]]:
    """Run `commits` in step, returning each one's revision or exception, and the workspace's secrets each time all of them wait together."""
    held: list[set[str]] = []
    barrier = threading.Barrier(
        len(commits), action=lambda: held.append(_secrets(scoped.store))
    )
    with ThreadPoolExecutor(len(commits)) as pool:
        futures = [
            pool.submit(
                claim.commit,
                _Overlapping(barrier, _SCHEMA, attributes={"p_nom": _long(value=v)}),
                credentials=key,
            )
            for claim, key, v in commits
        ]
        wait(futures)
    outcomes = [f.exception() or f.result() for f in futures]
    return outcomes, held


def test_two_commits_at_once_each_write_with_their_own_key(scoped: _Scoped):
    """Both layers' secrets coexist while both commits write, and each key reaches only its own layer."""
    children, held = _commit_at_once(
        scoped,
        [(scoped.claim, scoped.key, 2.0), (scoped.other, scoped.other_key, 3.0)],
    )
    assert held and all(len(names) == 3 for names in held), "both secrets coexist"
    values = [list(relation(child, "p_nom").df()["value"]) for child in children]
    assert values == [[2.0], [3.0]]
    assert _secrets(scoped.store) == {"datarecord_s3"}, "both layers' secrets are gone"


def test_two_commits_of_one_claim_at_once_race_to_its_manifest(scoped: _Scoped):
    """Each commit holds its own secret for the layer, so the second to write the manifest fails on it."""
    outcomes, held = _commit_at_once(
        scoped, [(scoped.claim, scoped.key, 2.0), (scoped.claim, scoped.key, 3.0)]
    )
    assert sorted(type(o).__name__ for o in outcomes) == [
        "PreconditionFailedError",
        "Revision",
    ], f"one commit wins the manifest: {outcomes}"
    assert len(scoped.store.revisions()) == 2, "the root and the winner"
    assert held and all(len(names) == 3 for names in held), "both secrets coexist"
    assert _secrets(scoped.store) == {"datarecord_s3"}, "both secrets are gone"


def test_a_layers_key_denied_a_write_leaves_no_secret(
    scoped: _Scoped, credentials: S3Credentials
):
    """DuckDB's write fails while the layer's secret exists, and the commit drops it."""
    _allow_only_their_layers(
        scoped.store, credentials, {"layer": scoped.claim.id}, denied=("s3:PutObject",)
    )
    with pytest.raises(duckdb.HTTPException):
        scoped.claim.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long(value=2.0)}),
            credentials=scoped.key,
        )
    assert scoped.store.revisions() == [scoped.root], "no layer committed"
    assert _secrets(scoped.store) == {"datarecord_s3"}, "the layer's secret is gone"


def test_a_workspace_without_a_key_reads_a_public_bucket_and_writes_with_a_layers(
    bucket, credentials, monkeypatch
):
    """Anonymous reads reach a bucket open to anyone, and only the layer's key writes."""
    admin = Workspace(credentials=credentials)
    try:
        store = admin.store(f"{bucket}/record")
        root = store.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None
        )
        _allow_only_their_layers(
            store, credentials, {"layer": store.claim(parent=root).id}, public=True
        )
    finally:
        admin.close()
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_CONFIG_FILE", os.devnull)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
    ws = Workspace()
    try:
        public = ws.store(store.uri)
        with pytest.raises(PermissionError, match="AccessDenied"):
            public.claim(parent=root)
        (claim,) = public.claims()
        child = claim.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long(value=2.0)}),
            credentials=replace(credentials, key_id="layer", secret="layer"),
        )
        assert child.ancestry() == [root.id, child.id]
        assert list(relation(child, "p_nom").df()["value"]) == [2.0]
    finally:
        ws.close()


def test_a_workspace_on_a_callers_connection_keeps_its_unnamed_secret(bucket):
    caller = duckdb.connect()
    caller.execute("INSTALL httpfs; LOAD httpfs;")
    caller.execute("CREATE SECRET (TYPE s3, KEY_ID 'caller', SECRET 'caller')")
    ws = Workspace(con=caller)
    ws.store(f"{bucket}/record")
    names = {n for (n,) in caller.sql("SELECT name FROM duckdb_secrets()").fetchall()}
    assert {"__default_s3", "datarecord_s3"} <= names, (
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
    assert not store.files.exists(store.layer_dir(UUID(pending))), (
        "every key under it went, though SeaweedFS keeps the directory"
    )
    with pytest.raises(ValueError, match=pending):
        store.cleanup(ids=[UUID(pending)])


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
