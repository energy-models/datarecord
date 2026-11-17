# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The workspace, its stores, and the tree at the location.

Notes
-----
- [the store](https://energy-models.github.io/datarecord/reference/store/#store)
- [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
"""

import json
import math
import os
import pickle
import re
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd
import pytest
from botocore.exceptions import ProfileNotFound
from botocore.stub import Stubber
from pydantic import ValidationError

from datarecord import Claim, Revision, S3Credentials, Workspace
from datarecord.mutable import WorkingRecord
from datarecord.schema.specs import Dimension
from datarecord.store import write
from datarecord.store.files import LocalFiles, PreconditionFailedError, RemoteFiles
from tests.fixtures import (
    broadcast,
    empty_child,
    empty_root,
    in_threads,
    nested_tables,
    schema,
    scratch_tables,
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
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
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

    Resolved files are not counted.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
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
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
    """
    claim = store.claim(parent=None)
    if committed:
        claim.commit(_p_nom())
        with pytest.raises(ValueError, match="committed already"):
            claim.commit(_p_nom())
    uri = store.claim_uri(claim.id)
    with pytest.raises(PreconditionFailedError):
        store._read_claim(claim.id).write(store.files, uri)


_KEY = "s3://bucket/record/layers/x/manifest.json"


@pytest.fixture
def s3(monkeypatch) -> Iterator[tuple[RemoteFiles, Stubber]]:
    """`RemoteFiles` whose S3 client answers only what the test stubs."""
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "stub")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "stub")
    files = RemoteFiles()
    with Stubber(files._s3) as stub:
        yield files, stub
        stub.assert_no_pending_responses()


def test_an_absent_s3_key_is_a_miss(s3):
    """S3 answers 404 for a key it does not hold, to a caller with ListBucket.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
    """
    files, stub = s3
    stub.add_client_error("get_object", "NoSuchKey", http_status_code=404)
    stub.add_client_error("head_object", "404", http_status_code=404)
    stub.add_response("list_objects_v2", {"KeyCount": 0})
    assert files.read_text(_KEY) is None
    assert not files.exists(_KEY), "a 404 on HEAD falls back to a listing"


def test_an_s3_directory_holding_no_key_is_absent(s3):
    """SeaweedFS keeps a directory once its keys are deleted, and answers a HEAD on it.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
    """
    files, stub = s3
    stub.add_response("list_objects_v2", {"KeyCount": 0})
    assert not files.exists("s3://bucket/record/layers/x/"), "only a listing decides"


@pytest.mark.parametrize(
    ("operations", "call"),
    [
        (["get_object"], lambda files: files.read_text(_KEY)),
        (["put_object"], lambda files: files.create(_KEY, "{}")),
        (["list_objects_v2"], lambda files: files.list(_KEY)),
        (["head_object"], lambda files: files.exists(_KEY)),
        (["head_object", "list_objects_v2"], lambda files: files.exists(_KEY)),
        (["list_objects_v2"], lambda files: files.delete(f"{_KEY}/")),
    ],
    ids=["read_text", "create", "list", "exists", "exists-listing", "delete"],
)
def test_a_denied_s3_call_raises_naming_the_uri(
    s3, operations: list[str], call: Callable[[RemoteFiles], object]
):
    """A 403 is no miss: a token scoped elsewhere fails loudly.

    Every operation but the last answers 404, the last 403.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
    """
    files, stub = s3
    *missed, denied = operations
    for operation in missed:
        stub.add_client_error(operation, "404", http_status_code=404)
    stub.add_client_error(denied, "AccessDenied", http_status_code=403)
    with pytest.raises(PermissionError, match=re.escape(_KEY)):
        call(files)


@pytest.mark.parametrize(
    ("codes", "raised"),
    [
        (["AccessDenied", "AccessDenied"], PermissionError),
        (["AccessDenied", "InternalError"], OSError),
    ],
    ids=["denied", "failed"],
)
def test_an_s3_delete_refused_key_by_key_raises(
    s3, codes: list[str], raised: type[OSError]
):
    """S3 answers a batch delete with 200 and an error per key it refused.

    Only where every refusal is `AccessDenied` is the delete denied.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
    """
    files, stub = s3
    keys = [f"record/layers/x/{i}" for i in range(len(codes))]
    stub.add_response("list_objects_v2", {"Contents": [{"Key": k} for k in keys]})
    errors = [{"Key": k, "Code": c} for k, c in zip(keys, codes, strict=True)]
    stub.add_response("delete_objects", {"Errors": errors})
    with pytest.raises(raised, match=re.escape("s3://bucket/record/layers/x/")) as e:
        files.delete("s3://bucket/record/layers/x/")
    assert type(e.value) is raised, "a mixed refusal is no PermissionError"


_KEY_ID = "AKIAEXAMPLE"
_SECRETS = ("wJalr-secret", "FwoGZ-token")


def test_s3_credentials_keep_the_secret_and_token_out_of_their_repr():
    credentials = S3Credentials(_KEY_ID, _SECRETS[0], session_token=_SECRETS[1])
    shown = f"{credentials!r} {credentials}"
    assert _KEY_ID in shown
    assert not any(s in shown for s in _SECRETS), "a log line shows no secret"


@pytest.mark.parametrize(
    ("given", "match"),
    [
        ({"key_id": ""}, "non-empty"),
        ({"secret": ""}, "non-empty"),
        ({"endpoint": "localhost:8333"}, "its scheme"),
        ({"endpoint": "http://localhost:8333/bucket"}, "no user, path"),
        ({"endpoint": "http://localhost:8333?x=1"}, "no user, path"),
        ({"endpoint": "http://localhost:8333#x"}, "no user, path"),
        ({"endpoint": "http://key:secret@localhost:8333"}, "no user, path"),
        ({"url_style": "virtual"}, "`url_style` is 'virtual'"),
    ],
    ids=[
        "key_id",
        "secret",
        "endpoint-scheme",
        "endpoint-path",
        "endpoint-query",
        "endpoint-fragment",
        "endpoint-userinfo",
        "url_style",
    ],
)
def test_s3_credentials_refuse_what_no_server_takes(given: dict[str, Any], match: str):
    with pytest.raises(ValueError, match=match):
        S3Credentials(**{"key_id": _KEY_ID, "secret": _SECRETS[0], **given})


def _secret(workspace: Workspace) -> dict[str, str]:
    """The workspace's DuckDB S3 secret, as DuckDB lists it with secrets redacted."""
    ((listed,),) = workspace.con.execute(
        "SELECT secret_string FROM duckdb_secrets() WHERE name = 'datarecord_s3'"
    ).fetchall()
    return dict(kv.split("=", 1) for kv in listed.split(";"))


@pytest.mark.parametrize(
    "endpoint", ["http://localhost:8333", "http://localhost:8333/"]
)
def test_s3_credentials_take_an_endpoint_with_a_trailing_slash(
    workspace, endpoint: str
):
    workspace.set_credentials(S3Credentials(_KEY_ID, _SECRETS[0], endpoint=endpoint))
    workspace.files("s3://bucket/record")
    assert _secret(workspace)["endpoint"] == "localhost:8333"


def test_injected_credentials_set_up_boto3_and_duckdb_alike(workspace, monkeypatch):
    """The environment's endpoint reaches neither, before or after the swap."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://elsewhere:9000")
    workspace.set_credentials(
        S3Credentials(
            _KEY_ID,
            _SECRETS[0],
            session_token=_SECRETS[1],
            endpoint="https://s3.example.org",
            region="eu-west-1",
            url_style="path",
        )
    )
    files = workspace.files("s3://bucket/record")
    assert isinstance(files, RemoteFiles)
    assert files._s3.meta.endpoint_url == "https://s3.example.org"
    assert files._s3.meta.region_name == "eu-west-1"
    secret = _secret(workspace)
    assert {k: secret[k] for k in ("provider", "key_id", "endpoint", "use_ssl")} == {
        "provider": "config",
        "key_id": _KEY_ID,
        "endpoint": "s3.example.org",
        "use_ssl": "true",
    }
    assert secret["secret"] == secret["session_token"] == "redacted"

    workspace.set_credentials(S3Credentials("other", "other"))
    files = workspace.files("s3://bucket/record")
    assert isinstance(files, RemoteFiles)
    assert files._s3.meta.endpoint_url == "https://s3.amazonaws.com"
    secret = _secret(workspace)
    assert (secret["key_id"], secret["url_style"]) == ("other", "vhost")
    assert "endpoint" not in secret and "session_token" not in secret


@pytest.mark.parametrize("source", ["environment", "credentials-file"])
def test_the_environments_key_sets_up_duckdb_without_a_config_file(
    workspace, monkeypatch, tmp_path, source: str
):
    """A self-hosted server's key, from the environment or a profile, with no `~/.aws/config`."""
    credentials = tmp_path / "credentials"
    credentials.write_text(
        f"[default]\naws_access_key_id = {_KEY_ID}\naws_secret_access_key = x\n"
    )
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    if source == "environment":
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", _KEY_ID)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRETS[0])
    else:
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing"))
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:8333")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    workspace.files("s3://bucket/record")
    secret = _secret(workspace)
    assert (secret["key_id"], secret["endpoint"]) == (_KEY_ID, "localhost:8333")


def _stale_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """boto3 refuses to build: `AWS_PROFILE` names a profile no config file has."""
    monkeypatch.setenv("AWS_PROFILE", "nonexistent")


def _empty_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """boto3 builds, keyless, and DuckDB's credential chain finds no key."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", os.devnull)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.mark.parametrize(
    ("environment", "replacement", "raised"),
    [
        (_stale_profile, S3Credentials("other", "other"), ProfileNotFound),
        (_empty_chain, None, duckdb.Error),
    ],
    ids=["boto3-refuses", "duckdb-refuses"],
)
def test_failed_replacement_keeps_the_old_credentials(
    workspace,
    monkeypatch,
    environment: Callable[[pytest.MonkeyPatch], None],
    replacement: S3Credentials | None,
    raised: type[Exception],
):
    workspace.set_credentials(S3Credentials(_KEY_ID, _SECRETS[0]))
    files = workspace.files("s3://bucket/record")
    environment(monkeypatch)
    import pathlib
    import sys

    print("DIAG platform", sys.platform, "devnull", repr(os.devnull))
    print("DIAG py env", {k: v for k, v in os.environ.items() if k.startswith("AWS")})
    os.environ["AWS_ACCESS_KEY_ID"] = "PROBEKEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "probesecret"
    probe = duckdb.connect()
    try:
        probe.execute(
            "CREATE SECRET p (TYPE s3, PROVIDER credential_chain, CHAIN 'env')"
        )
        print(
            "DIAG env probe",
            probe.execute("SELECT secret_string FROM duckdb_secrets()").fetchall(),
        )
    except duckdb.Error as e:
        print("DIAG env probe raised", e)
    del os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]
    aws = pathlib.Path.home() / ".aws"
    print("DIAG ~/.aws", aws, aws.exists() and sorted(p.name for p in aws.iterdir()))
    try:
        workspace.set_credentials(replacement)
    except raised as e:
        print("DIAG raised", type(e).__name__, e)
    else:
        print("DIAG no raise; secret", _secret(workspace))
        print(
            "DIAG secrets",
            workspace.con.execute(
                "SELECT name, provider, secret_string FROM duckdb_secrets()"
            ).fetchall(),
        )
        print(
            "DIAG extensions",
            workspace.con.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded"
            ).fetchall(),
        )
    with pytest.raises(raised):
        workspace.set_credentials(replacement)
    assert workspace.files("s3://bucket/record") is files
    assert _secret(workspace)["key_id"] == _KEY_ID, "boto3 and DuckDB stay alike"


def test_closing_a_workspace_drops_its_secret_from_a_callers_connection():
    caller = duckdb.connect()
    ws = Workspace(con=caller, credentials=S3Credentials(_KEY_ID, _SECRETS[0]))
    ws.files("s3://bucket/record")
    ws.close()
    names = caller.execute("SELECT name FROM duckdb_secrets()").fetchall()
    assert names == [], "the caller's connection outlives the workspace's secret"
    caller.close()


def test_credentials_on_a_local_workspace_set_up_no_secret(workspace, tmp_path):
    """No `httpfs` and no secret until the first `s3://` URI."""
    workspace.set_credentials(S3Credentials(_KEY_ID, _SECRETS[0]))
    workspace.store(str(tmp_path)).commit(
        _Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None
    )
    names = workspace.con.execute("SELECT name FROM duckdb_secrets()").fetchall()
    assert ("datarecord_s3",) not in names


def test_a_key_for_its_layer_is_refused_on_a_local_store(store):
    claim = store.claim(parent=None)
    with pytest.raises(ValueError, match="is local, so it takes no S3 credentials"):
        claim.commit(
            _Source(_SCHEMA, attributes={"p_nom": _long()}),
            credentials=S3Credentials(_KEY_ID, _SECRETS[0]),
        )
    assert store.claims() == [claim], "the layer stays pending"


def test_a_pending_layer_is_invisible_to_every_read(workspace, store):
    """A layer holding a claim and data but no manifest is no revision, but a claim.

    Its commit failed after writing its axis, so its files are there.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
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
        lambda _: {"older_than": timedelta(0)},
    ],
    ids=["ids", "age"],
)
def test_cleanup_deletes_a_pending_layer_and_no_committed_one(store, by):
    """Named by id, or claimed at least `older_than` ago, which `timedelta(0)` reaches.

    Notes
    -----
    - [cleanup](https://energy-models.github.io/datarecord/reference/store/#cleanup)
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
    - [cleanup](https://energy-models.github.io/datarecord/reference/store/#cleanup)
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
    - [cleanup](https://energy-models.github.io/datarecord/reference/store/#cleanup)
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
    - [cleanup](https://energy-models.github.io/datarecord/reference/store/#cleanup)
    """
    claim = store.claim(parent=None)
    stray = Path(store.layer_dir(uuid4()))
    stray.mkdir()
    (stray / ".claim.json.abc123").write_text("{}")
    assert [c.id for c in store.claims()] == [claim.id]
    assert store.cleanup(older_than=timedelta(0)) == [claim.id]
    assert stray.exists(), "only a claimed layer is aged"


@pytest.fixture(scope="module")
def metas(module_store):
    """Six revisions by their `meta`, and a pending claim carrying `a`'s."""
    metas = {
        "a": {"run": "a", "n": 1},
        "b": {"run": "b", "n": 1},
        "c": {"run": "c", "ok": True},
        "d": {"run": "d", "changes": {"p_nom": {"n": 1, "rows": [1, 2, 3]}}},
        "e": {"run": "e", "note": {"meta": {"run": "x"}}},
        "f": {"run": "f", "notes": [{"meta": {"run": "x"}}]},
    }
    names = {
        module_store.commit(_p_nom(), parent=None, meta=m).id: name
        for name, m in metas.items()
    }
    module_store.claim(parent=None, meta=metas["a"])
    return module_store, names


@pytest.mark.parametrize(
    ("meta", "found"),
    [
        ({}, {"a", "b", "c", "d", "e", "f"}),
        ({"run": "a", "n": 1}, {"a"}),
        ({"run": "a", "n": 2}, set()),
        ({"n": 1.0}, set()),
        ({"changes": {"p_nom": {"n": 1}}}, {"d"}),
        ({"changes": {"p_nom": {"rows": [3, 1]}}}, {"d"}),
        ({"changes": {"p_nom": {"rows": [1, 1]}}}, {"d"}),
        ({"changes": {"p_nom": {"rows": [4]}}}, set()),
        ({"changes": {"p_nom": {"n": 1.0}}}, set()),
        ({"p_nom": {"n": 1}}, set()),
        ({"changes": {}}, {"d"}),
        ({"run": "x"}, set()),
    ],
    ids=[
        "none",
        "both",
        "one-off",
        "other-type",
        "nested-pair",
        "array-subset",
        "array-repeated",
        "array-other",
        "nested-other-type",
        "not-below-the-top",
        "empty-object",
        "not-a-nested-meta-key",
    ],
)
def test_revisions_filter_by_meta(metas, meta, found):
    """A revision is listed when its `meta` contains the given meta; a pending layer never.

    Containment is key by key from the top, so a given array matches a
    stored one holding its elements in any order, and a pair matches no
    deeper object.

    Notes
    -----
    - [the layers](https://energy-models.github.io/datarecord/reference/store/)
    """
    store, names = metas
    fresh = Workspace()
    try:
        for reader in (store, fresh.store(store.uri)):
            listed = {names[r.id] for r in reader.revisions(meta=meta)}
            assert listed == found, "the same from the cache as from the manifests"
    finally:
        fresh.close()


@pytest.mark.parametrize(
    ("meta", "match"),
    [
        ({"x": math.nan}, "finite"),
        ({"x": {"y": math.nan}}, "finite"),
        ({"x": None}, "input_value=None"),
        ({"x": [None]}, "input_value=None"),
    ],
    ids=["nan", "nested-nan", "null", "nested-null"],
)
def test_a_meta_value_is_finite_and_not_null(store, meta, match):
    """JSON has no NaN or infinity, which would be read back as null, and null is refused.

    Refused at any depth, before the claim or the manifest is written.

    Notes
    -----
    - [the layers](https://energy-models.github.io/datarecord/reference/store/)
    """
    with pytest.raises(ValidationError, match=match):
        store.claim(parent=None, meta=meta)
    assert not Path(store.uri, "layers").exists(), "no claim is written"
    claim = store.claim(parent=None)
    with pytest.raises(ValidationError, match=match):
        claim.commit(_p_nom(), meta=meta)
    assert _pending(store) == [str(claim.id)], "no manifest is written"


def test_a_claim_crosses_a_process_boundary(store):
    """Serialised with all five fields, or as its location and id read back from its claim file.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/reference/store/#claim)
    """
    root = store.commit(_p_nom(), parent=None)
    claimed = {"run": "a", "by": {"names": ["x", "y"], "n": 1.5}}
    claim = store.claim(parent=root, meta=claimed)
    dumped = claim.model_dump_json()
    assert json.loads(dumped).keys() == {"id", "store", "parent", "meta", "created"}
    short = json.dumps({"id": str(claim.id), "store": store.uri})

    for back in (
        Claim.model_validate_json(dumped),
        Claim.model_validate_json(short),
        pickle.loads(pickle.dumps(claim)),
    ):
        assert back.store is Workspace.process().store(store.uri)
        assert (back.id, back.parent, back.meta, back.created) == (
            claim.id,
            root.id,
            claimed,
            claim.created,
        )
    done = pickle.loads(pickle.dumps(claim)).commit(_p_nom(2.0), meta={"ok": True})
    assert store.revision(done.id).meta == {**claimed, "ok": True}
    with pytest.raises(KeyError, match="No claim"):
        Claim.model_validate({"id": str(uuid4()), "store": store.uri})


@pytest.mark.parametrize("form", ["full", "short"])
def test_a_claim_rebuilds_on_a_store_the_caller_names(store, form):
    """The full form is taken as it stands, without a read; the short form reads `claim.json`.

    The payload's `meta` differs from the claim file's, so the commit shows
    which one the rebuilt claim holds; its location has a trailing slash.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/reference/store/#claim)
    """
    root = store.commit(_p_nom(), parent=None)
    claim = store.claim(parent=root, meta={"run": "a"})
    full = {
        **json.loads(claim.model_dump_json()),
        "store": f"{store.uri}/",
        "meta": {"run": "b"},
    }
    payload = full if form == "full" else {"id": full["id"], "store": full["store"]}
    back = store.claim_from(json.dumps(payload))
    assert back.store is store, "the named store, not the process workspace's"
    assert (back.id, back.parent, back.created) == (claim.id, root.id, claim.created)
    meta = {"run": "b"} if form == "full" else {"run": "a"}
    assert back.meta == meta
    assert back.commit(_p_nom(2.0)).meta == meta


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({"store": "elsewhere"}, ValueError),
        ({"store": None}, ValueError),
        ({"store": 5}, ValueError),
        ("[1]", ValueError),
        ("5", ValueError),
        ({"id": str(uuid4())}, KeyError),
        ({"parent": None}, ValidationError),
        (
            {"parent": None, "meta": {}, "created": "2026-11-05T17:37:14"},
            ValidationError,
        ),
    ],
    ids=[
        "another-store",
        "no-store",
        "store-no-string",
        "a-list",
        "a-number",
        "no-claim",
        "a-field-missing",
        "created-naive",
    ],
)
def test_a_claim_from_a_bad_payload_is_refused(store, payload, error):
    """Over a short form `{id, store}` of a pending claim, or JSON text as it stands.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/reference/store/#claim)
    """
    claim = store.claim(parent=None)
    short = {"id": str(claim.id), "store": store.uri}
    text = payload if isinstance(payload, str) else json.dumps({**short, **payload})
    with pytest.raises(error):
        store.claim_from(text)


def test_a_claim_on_a_pending_parent_commits_after_it(store):
    """A chain claimed up front commits in order, and a child before its parent waits.

    Notes
    -----
    - [the claim](https://energy-models.github.io/datarecord/reference/store/#claim)
    """
    root = store.commit(_p_nom(), parent=None)
    first = store.claim(parent=root)
    second = store.claim(parent=first)
    with pytest.raises(KeyError, match=str(first.id)):
        second.commit(_p_nom(3.0))
    assert sorted(_pending(store)) == sorted([str(first.id), str(second.id)])
    first.commit(_p_nom(2.0))
    assert second.commit(_p_nom(3.0)).ancestry() == [root.id, first.id, second.id]


_CLAIMED = {"run": "a", "n": 3, "by": {"user": "u", "tags": ["a"]}}
_CHANGES = {"p_nom_max": {"subjects": 412, "series_rows": 0, "median_ratio": 1.5}}


@pytest.mark.parametrize(
    ("given", "outcome"),
    [
        ({"status": "optimal"}, {**_CLAIMED, "status": "optimal"}),
        ({"manta.changes": _CHANGES}, {**_CLAIMED, "manta.changes": _CHANGES}),
        ({"by": {"user": "u", "tags": ["a"]}}, _CLAIMED),
        ({"run": "b"}, "changes the claimed meta \\['run'\\]"),
        ({"n": 3.0}, "changes the claimed meta \\['n'\\]"),
        ({"by": {"user": "v", "tags": ["a"]}}, "changes the claimed meta \\['by'\\]"),
        ({"by": {"user": "u", "tags": []}}, "changes the claimed meta \\['by'\\]"),
    ],
    ids=[
        "adds",
        "adds-nested",
        "restates-nested",
        "changes",
        "changes-type",
        "changes-nested",
        "narrows-nested",
    ],
)
def test_a_commit_may_add_to_its_claims_meta_but_not_change_it(store, given, outcome):
    """A job learns its outcome at its end, so it adds a key; a claimed key stays.

    Values compare as JSON, so `3` is no `3.0` and an object changes with
    any key inside it. Read back by a new workspace, each value keeps its type.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
    """
    claim = store.claim(parent=None, meta=_CLAIMED)
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
    - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
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


def test_revisions_sharing_a_schema_share_one_object(store):
    """Parsed once per workspace, by digest, whether first written or first read."""
    root = store.commit(_p_nom(), parent=None)
    child = store.commit(_p_nom(), parent=root)
    wide = widened(_SCHEMA, "extra")
    leaf = store.commit(_Source(wide, attributes={"p_nom": _long()}), parent=child)
    fresh = Workspace()
    try:
        for s in (store, fresh.store(store.uri)):
            schemas = [s.revision(r.id).manifest.schema for r in (root, child, leaf)]
            assert schemas[0] is schemas[1], "one object for one schema"
            assert schemas[2] is not schemas[0], "a widened schema is its own"
    finally:
        fresh.close()


def _standalone(store, root, tmp_path):
    return store.workspace.write(root.record, str(tmp_path / "flat"))


def _another_stores(store, root, tmp_path):  # noqa: ARG001
    other = store.workspace.store(str(tmp_path / "other"))
    return other.commit(_p_nom(), parent=None).record


def test_a_working_record_lands_on_its_base(store):
    """`store.commit(w)` writes `w`'s edits as a child of the revision `w` was built over.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
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


@pytest.mark.parametrize("base", [_standalone, _another_stores])
def test_a_working_record_over_no_revision_of_the_store_is_refused(
    store, tmp_path, base
):
    """Over anything but a revision of the store, its edits have no parent there.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """
    root = store.commit(_p_nom(), parent=None)
    elsewhere = WorkingRecord(base(store, root, tmp_path))
    with pytest.raises(ValueError, match=r"`ws\.write\(w, uri\)`"):
        store.commit(elsewhere)
    assert [r.id for r in store.revisions()] == [root.id]


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
    - [crossing a process boundary](https://energy-models.github.io/datarecord/reference/store/#revision)
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
    """Each thread reads on its own cursor, and the fold is done once, keeping no step's table.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
    """
    _, child = tree
    record = child.record

    def read(_):
        rows = record.attributes["p_max_pu"].fetchall()
        return workspace.con, len(rows)

    results = in_threads(8, read)
    cursors = {id(con) for con, _ in results}
    assert len(cursors) == 8, "one cursor per thread"
    assert len({n for _, n in results}) == 1, "every thread reads the same rows"
    folds = nested_tables(workspace.con, "_fold")
    assert "owner_map" in folds[child.id.hex], "the map is a shared table"
    child.materialise()
    cons = [workspace.con, *(con for con, _ in results)]
    assert all(scratch_tables(con) == set() for con in cons), (
        "the fold keeps no table of its steps on any cursor"
    )


@pytest.mark.parametrize("working", [False, True], ids=["frozen", "working"])
def test_a_record_read_first_here_reads_alike_in_other_threads(tree, working):
    """What one thread keeps of a record's fold plans no relation on another's cursor.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
    """
    _, child = tree
    record = WorkingRecord(child.record) if working else child.record
    here = _read_all(record)
    assert in_threads(4, lambda _: _read_all(record)) == [here] * 4


def test_two_threads_missing_the_fold_at_once_both_get_it(store, tree):
    _, child = tree

    def fold(_):
        fresh = store.revision(child.id)
        return sorted(fresh.record._fold.owner_map.fetchall(), key=str)

    first, second = in_threads(2, fold)
    assert first == second
    assert first


def test_a_revision_is_cached_only_after_its_parent(store):
    """Threads reading one tree into a fresh workspace each read the leaf's whole ancestry.

    Through `revisions()` or the leaf alone, a thread that finds the leaf
    cached finds every ancestor cached too.

    Notes
    -----
    - [the layers](https://energy-models.github.io/datarecord/reference/store/)
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
            chains = in_threads(8, partial(read, again))
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
        ids = in_threads(2, lambda _: again.revision(child.id).id)
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
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
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
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
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
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
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
    labels = after.record.dimensions["scenario"].df()["scenario"].tolist()
    assert labels == ["low", "high"], "the axis is the list, in its order"


def test_a_failed_materialise_leaves_the_node_unmaterialised(store, tree, monkeypatch):
    """`owner_map.parquet` marks a node materialised, so it is written last.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    from datarecord.store import materialise

    root, _ = tree

    def fail(*args, **kwargs):
        raise RuntimeError("the owner map write fails")

    monkeypatch.setattr(materialise, "_checkpointed", fail)
    with pytest.raises(RuntimeError):
        root.materialise()
    assert Path(store.dimension_owners_uri(root.id)).exists()
    assert not materialise.materialised(root.id, store), (
        "the rest does not make it materialised"
    )
    assert not list(Path(store.resolved_dir(root.id)).rglob("*.tmp")), (
        "no temporary file is left behind"
    )


def test_a_relation_from_another_connection_is_rejected(tree):
    """Copying it over would read it on a connection nobody asked to read.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
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


def _read_all(record):
    """Every dim, attribute and flag of `record`, rows sorted so two folds compare."""
    return (
        {d: sorted(r.fetchall(), key=str) for d, r in record.dimensions.items()},
        {a: sorted(r.fetchall(), key=str) for a, r in record.attributes.items()},
        record.flags(),
    )


@pytest.mark.parametrize("held", [("dimensions",), ("dimensions", "attributes")])
def test_a_commits_child_reads_as_folded_afresh(workspace, store, tree, held):
    """A record on a commit's child folds onto the fold its workspace holds of the parent.

    The edits read before the commit leave the parent's axes, and with an
    attribute its owner map, as the workspace's tables.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    _, child = tree
    w = WorkingRecord(child.record)
    w.add("entity", ["solar"], ctype="Generator")
    w.add("scenario", ["high", "low"])
    w.set("p_max_pu", 0.7, entity="solar")
    w.set("p_max_pu", 0.9, scenario="high")
    w.remove("entity", ["wind"])
    relations = {"dimensions": w.dimensions, "attributes": w.attributes}
    for kind in held:
        for rel in relations[kind].values():
            rel.fetchall()
    grandchild = store.commit(w)
    fresh = Workspace()
    try:
        afresh = _read_all(fresh.store(store.uri).revision(grandchild.id).record)
        assert _read_all(grandchild.record) == afresh
    finally:
        fresh.close()


@pytest.mark.parametrize("read", [False, True], ids=["cold", "held"])
def test_a_materialised_node_reads_as_folded_afresh(store, tree, read):
    """A node materialises the fold its workspace holds, or folds afresh, to the same resolved files.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    _, child = tree
    w = WorkingRecord(child.record)
    w.add("scenario", ["high", "low"])
    w.set("p_max_pu", 0.9, scenario="high")
    node = store.commit(w)
    leaf = empty_child(node).commit()
    if read:
        _read_all(node.record)
    expected = _read_all(leaf.record)
    node.materialise()
    fresh = Workspace()
    try:
        again = fresh.store(store.uri).revision(leaf.id).record
        assert _read_all(again) == expected
    finally:
        fresh.close()
