# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The `Workspace` holding the connection, the `Store` holding a location, and its `Revision`s.

A workspace is the DuckDB database and the means to reach files, which is all a
standalone directory needs. A store is one location - its layers, each carrying
its manifest - and works through a workspace several stores share. A revision
is one node of that tree, and holds no state beyond its place in it.

Notes
-----
- [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
- [layered resolution](https://energy-models.github.io/datarecord/design/layers/tree/)
- [module layout](https://energy-models.github.io/datarecord/design/package/)
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Annotated, Any, overload
from urllib.parse import urlparse
from uuid import UUID, uuid4, uuid5

import duckdb
from duckdb import DuckDBPyConnection
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    PrivateAttr,
    model_validator,
)

from datarecord.mutable import StagedSource, WorkingRecord
from datarecord.protocols import LayerData, RecordLike
from datarecord.resolve.record import Record
from datarecord.resolve.sources import DirectorySource
from datarecord.schema.schema import Schema
from datarecord.store.cache import materialise, sources_to_read
from datarecord.store.files import Files, LocalFiles, RemoteFiles
from datarecord.store.manifest import (
    ClaimFile,
    LayerFiles,
    Manifest,
    Meta,
    merge_meta,
    validate_meta,
)
from datarecord.store.write import layer_data, write_layer

#: Seconds `Store._insert` retries a write-write conflict before giving up.
_INSERT_TIMEOUT = 10.0

_SCHEMAS = """
CREATE TABLE IF NOT EXISTS schemas (
  hash   VARCHAR PRIMARY KEY,
  schema JSON
)
"""

_REVISIONS = """
CREATE TABLE IF NOT EXISTS revisions (
  location VARCHAR,
  id       UUID,
  parent   UUID,
  meta     JSON,
  files    STRUCT(dims VARCHAR[], attributes VARCHAR[]),
  schema   VARCHAR,
  PRIMARY KEY (location, id)
)
"""


class _Cursors(threading.local):
    """The calling thread's cursor on one database.

    `threading.local` reruns `__init__` in each thread that first touches it, so
    there is no existence check and no lock, and a thread's cursor goes with it.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """

    def __init__(self, db: DuckDBPyConnection) -> None:
        self.con = db.cursor()


class Workspace:
    """A DuckDB database and the files it reaches, shared by every store on it.

    `con` is the calling thread's cursor: one workspace serves every thread, and
    their work shares one database - its tables, never its relations.

    Parameters
    ----------
    database
        DuckDB database, `:memory:` by default. What lives there is a workspace
        and never the record: deleting it loses at most uncommitted edits.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """

    def __init__(self, database: str = ":memory:") -> None:
        self._db = duckdb.connect(database)
        self._db.execute(_SCHEMAS)
        self._db.execute(_REVISIONS)
        self._cursors = _Cursors(self._db)
        self._stores: dict[str, Store] = {}
        self._local = LocalFiles()
        self._remote: Files | None = None
        self._remote_lock = threading.Lock()

    @property
    def con(self) -> DuckDBPyConnection:
        """The calling thread's cursor, made on its first use in that thread."""
        return self._cursors.con

    @classmethod
    def process(cls) -> Workspace:
        """The process's workspace, created on first use on `:memory:`.

        What a deserialised `Revision` or `Claim` attaches to, having no
        workspace passed; nothing else reaches it unless named.

        Notes
        -----
        - [crossing a process boundary](https://energy-models.github.io/datarecord/design/storage/concurrency/#crossing-a-process-boundary)
        """
        global _process
        with _process_lock:
            if _process is None:
                _process = cls()
            return _process

    def files(self, uri: str) -> Files:
        """The `Files` for `uri`'s scheme: local without `://`, remote for `s3://`.

        Raises
        ------
        ValueError
            For any other scheme.
        """
        if "://" not in uri:
            return self._local
        if not uri.startswith("s3://"):
            msg = f"{uri}: a location is a local path or an s3:// URI"
            raise ValueError(msg)
        with self._remote_lock:
            if self._remote is None:
                self._remote = self._connect_remote()
            return self._remote

    def _connect_remote(self) -> Files:
        """Set up boto3 and DuckDB's `httpfs` from the AWS environment, once.

        Never for a local workspace: `PROVIDER credential_chain` probes
        instance-metadata endpoints that hang for minutes where none exist.
        With `AWS_ENDPOINT_URL` set, the secret adds what the chain does not
        supply - the endpoint, path-style addressing, SSL per its scheme - and
        skips that probe, a self-hosted server having no metadata service.

        Raises
        ------
        ImportError
            If boto3, from `datarecord[remote]`, is missing.

        Notes
        -----
        - [files, local and remote](https://energy-models.github.io/datarecord/design/storage/store/#files-local-and-remote)
        """
        files = RemoteFiles()
        options = ["TYPE s3", "PROVIDER credential_chain"]
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:
            url = urlparse(endpoint)
            options += [
                f"ENDPOINT '{url.netloc}'",
                "URL_STYLE 'path'",
                f"USE_SSL {str(url.scheme == 'https').lower()}",
                "CHAIN 'env;config'",
            ]
        self._db.execute("INSTALL httpfs; LOAD httpfs;")
        self._db.execute(f"CREATE OR REPLACE SECRET ({', '.join(options)})")
        return files

    def store(self, uri: str) -> Store:
        """This workspace's store at `uri`, one object per normalised location.

        Raises
        ------
        ValueError
            For an empty `uri`.
        """
        if not uri:
            msg = (
                "a store needs a location; an empty one would be the working directory"
            )
            raise ValueError(msg)
        location = _absolute(uri)
        self.files(location)
        return self._stores.setdefault(location, Store(location, self))

    def at(self, uri: str) -> Record:
        """The standalone record at `uri`, read under its own `manifest.json`.

        A local `uri` is resolved, as `store` resolves one, so a later `chdir`
        cannot point it at another record.

        Raises
        ------
        FileNotFoundError
            If `uri` has no manifest, as where its write failed.
        ValueError
            If the manifest names a parent: a layer of a tree is read alone
            through its store's `layer(id)`.

        Notes
        -----
        - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
        """
        base = _absolute(uri) + "/"
        manifest = Manifest.read(self.files(base), base + "manifest.json")
        if manifest is None:
            msg = f"{base} has no manifest.json, so holds no record"
            raise FileNotFoundError(msg)
        if manifest.parent is not None:
            msg = (
                f"{base} is a layer over revision {manifest.parent}; a layer of a "
                f"tree is read alone through its store's `layer(id)`"
            )
            raise ValueError(msg)
        return _one_layer(base, manifest, self, manifest.id)

    def write(
        self, source: LayerData | RecordLike, uri: str, *, meta: Meta | None = None
    ) -> Record:
        """Write `source` as a standalone record at `uri`, and read it back.

        A standalone record is one layer at its own URI, written to a URI that
        does not exist yet: its data first, then its manifest, with a fresh id
        and no parent, so one whose write failed reads as no record and one
        written again elsewhere is never read as the one before it. A failed
        write leaves its data files at `uri`, locally as remotely, for the
        caller to delete before writing there again.

        Raises
        ------
        FileExistsError
            If anything exists at `uri`: locally the path, even an empty
            directory; remotely any key under it.

        Notes
        -----
        - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
        - [a standalone directory](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-standalone-directory)
        """
        base = _absolute(uri) + "/"
        meta = validate_meta(meta)
        if self.files(base).exists(base):
            msg = (
                f"{base} already exists; a standalone record is written to a URI "
                f"that does not exist yet"
            )
            raise FileExistsError(msg)
        data = layer_data(source, self.con)
        schema = data.schema
        files = write_layer(data, base, self)
        manifest = Manifest(
            id=uuid4(), parent=None, schema=schema, files=files, meta=meta
        )
        manifest.write(self.files(base), base + "manifest.json")
        return self.at(base)

    def close(self) -> None:
        """Close the database, and every thread's cursor with it."""
        self._db.close()


_process: Workspace | None = None
_process_lock = threading.Lock()


class _Base(Enum):
    """`Store.commit`'s `parent` left out, which only a `WorkingRecord` may do."""

    OF_WORKING_RECORD = auto()


class Store:
    """One store location: its layers, each carrying the manifest that makes it a revision.

    Holds no connection of its own: every read and write goes through its
    workspace, which several stores share.

    Attributes
    ----------
    uri
        The location: absolute, never empty.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """

    def __init__(self, uri: str, workspace: Workspace) -> None:
        self.uri = uri
        self.workspace = workspace

    def __repr__(self) -> str:
        return f"Store({self.uri!r})"

    def __reduce__(self) -> tuple[Any, ...]:
        return _process_store, (self.uri,)

    @property
    def files(self) -> Files:
        return self.workspace.files(self.uri)

    # -- paths ---------------------------------------------------------------

    def layer_dir(self, id: UUID | str) -> str:
        """Revision `id`'s layer directory, with a trailing slash.

        Notes
        -----
        - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
        """
        return f"{self.uri}/layers/{id}/"

    def manifest_uri(self, id: UUID | str) -> str:
        """Layer `id`'s manifest, which makes it a revision."""
        return f"{self.layer_dir(id)}manifest.json"

    def claim_uri(self, id: UUID | str) -> str:
        """Layer `id`'s claim, which handed its id out once."""
        return f"{self.layer_dir(id)}claim.json"

    def axis_uri(self, id: UUID | str, dim: str) -> str:
        """Where revision `id`'s layer holds `dim`'s axis.

        Notes
        -----
        - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
        """
        return f"{self.layer_dir(id)}dims/{dim}.parquet"

    def resolved_dir(self, id: UUID | str) -> str:
        """Revision `id`'s node caches, under its layer where no manifest lists them.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        return f"{self.layer_dir(id)}resolved/"

    def owner_map_uri(self, id: UUID | str) -> str:
        """Where revision `id`'s owner map is materialised.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        return f"{self.resolved_dir(id)}owner_map.parquet"

    def dim_owners_uri(self, id: UUID | str) -> str:
        """Where revision `id`'s dim owners are materialised.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        return f"{self.resolved_dir(id)}dim_owners.parquet"

    # -- writing -------------------------------------------------------------

    def claim(self, *, parent: Revision | None, meta: Meta | None = None) -> Claim:
        """Hand out a new layer id under `parent`, by its create-only `claim.json`.

        The layer is pending, reached by no read, until `Claim.commit` writes
        its manifest; `meta` is the revision's, which the commit may extend
        but not change.

        Raises
        ------
        ValueError
            If `parent` is a revision of another store, naming both locations.
        pydantic.ValidationError
            If `meta` is not flat, each value a string, finite number or boolean.

        Notes
        -----
        - [a revision exists only by its file](https://energy-models.github.io/datarecord/design/storage/store/#a-revision-exists-only-by-its-file)
        """
        if parent is not None and parent.store.uri != self.uri:
            msg = (
                f"revision {parent.id} is of the store at {parent.store.uri}, so "
                f"it is no parent in the store at {self.uri}"
            )
            raise ValueError(msg)
        claimed = ClaimFile(
            id=uuid4(),
            parent=None if parent is None else parent.id,
            meta=validate_meta(meta),
            created=datetime.now(UTC),
        )
        claimed.write(self.files, self.claim_uri(claimed.id))
        return Claim(store=self, **asdict(claimed))

    @overload
    def commit(
        self, source: WorkingRecord, *, meta: Meta | None = None
    ) -> Revision: ...

    @overload
    def commit(
        self,
        source: LayerData | RecordLike,
        *,
        parent: Revision | None,
        meta: Meta | None = None,
    ) -> Revision: ...

    def commit(
        self,
        source: LayerData | RecordLike,
        *,
        parent: Revision | None | _Base = _Base.OF_WORKING_RECORD,
        meta: Meta | None = None,
    ) -> Revision:
        """Write `source` as a new revision under `parent`: a claim and its commit in one.

        A `WorkingRecord` brings its parent, the revision it was built over,
        and is written as its edits, a delta against that revision. Any other
        source names its `parent`, `None` for a new root, and is written
        whole.

        Raises
        ------
        TypeError
            If a `WorkingRecord` comes with a `parent`, or any other source
            without one.
        ValueError
            If `w`'s base is no revision of this store, such as a standalone
            directory; as `claim` and `Claim.commit` do.

        Notes
        -----
        - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
        - [edits](https://energy-models.github.io/datarecord/design/use/editing/#edits)
        """
        if isinstance(source, WorkingRecord):
            if parent is not _Base.OF_WORKING_RECORD:
                msg = (
                    "a `WorkingRecord` is written over the revision it was built "
                    "over, so it takes no `parent`"
                )
                raise TypeError(msg)
            parent = self._base_of(source)
        elif parent is _Base.OF_WORKING_RECORD:
            msg = "`commit` needs a `parent=`, `None` for a new root"
            raise TypeError(msg)
        return self.claim(parent=parent, meta=meta).commit(source)

    def _base_of(self, w: WorkingRecord) -> Revision:
        """The revision of this store `w` was built over.

        Raises
        ------
        ValueError
            Naming `ws.write(w, uri)`, if `w` was built over anything else.
        """
        try:
            return self.revision(w.revision_id)
        except KeyError:
            msg = (
                f"this `WorkingRecord` was built over {w.revision_id}, no revision "
                f"of the store at {self.uri}, so its edits have no parent here; "
                f"write it whole with `ws.write(w, uri)`"
            )
            raise ValueError(msg) from None

    # -- the tree ------------------------------------------------------------

    def _cache(self, manifests: Iterable[Manifest]) -> None:
        """Add `manifests` to the workspace's `revisions` table in order, their schemas to `schemas`.

        Each schema row goes in before its revision row, which `lineage` joins it to.
        """
        for manifest in manifests:
            text = manifest.schema.model_dump_json()
            digest = hashlib.sha256(text.encode()).hexdigest()
            files = {
                "dims": list(manifest.files.dims),
                "attributes": list(manifest.files.attributes),
            }
            self._insert("INSERT OR IGNORE INTO schemas VALUES (?, ?)", [digest, text])
            self._insert(
                "INSERT OR IGNORE INTO revisions VALUES (?, ?, ?, ?, ?, ?)",
                [
                    self.uri,
                    manifest.id,
                    manifest.parent,
                    json.dumps(manifest.meta),
                    files,
                    digest,
                ],
            )

    def _insert(self, query: str, params: list[Any]) -> None:
        """Run `query`, an `INSERT OR IGNORE` of a row that never changes once written.

        Two cursors adding one row at once write the same thing, so the loser
        is done once it fails with a duplicate key, at insert or at commit. It
        fails with a conflict while the winner's row is not yet visible, and
        retries until it is.

        Raises
        ------
        TimeoutError
            If the conflict outlasts `_INSERT_TIMEOUT` seconds.
        """
        deadline = time.monotonic() + _INSERT_TIMEOUT
        while True:
            try:
                self.workspace.con.execute(query, params)
            except duckdb.TransactionException as e:
                if "duplicate key" in str(e):
                    return
                if "conflict" not in str(e).lower():
                    raise
                if time.monotonic() > deadline:
                    msg = f"a write-write conflict on {params[:2]} outlasted {_INSERT_TIMEOUT}s"
                    raise TimeoutError(msg) from e
                time.sleep(0.001)
                continue
            except duckdb.ConstraintException:
                pass
            return

    def _read_manifest(self, id: UUID) -> Manifest | None:
        """Layer `id`'s manifest, or `None` for a pending layer or none at all.

        Raises
        ------
        ValueError
            If the manifest names another id than its directory's.
        """
        manifest = Manifest.read(self.files, self.manifest_uri(id))
        if manifest is not None and manifest.id != id:
            msg = f"{self.manifest_uri(id)} names revision {manifest.id}"
            raise ValueError(msg)
        return manifest

    def _held(self, id: UUID) -> tuple[UUID] | None:
        return self.workspace.con.execute(
            "SELECT id FROM revisions WHERE location = ? AND id = ?",
            [self.uri, id],
        ).fetchone()

    def _cache_upward(self, id: UUID, read: Mapping[UUID, Manifest | None]) -> None:
        """Cache `id`'s manifest and each ancestor's not yet held, root first.

        A row goes in only once its parent's is there, so no thread ever sees a
        held revision whose ancestry `lineage` would cut short. A manifest
        missing from `read`, or `None` there as read before it committed, is
        read again.

        Raises
        ------
        KeyError
            If `id` or an ancestor has no manifest.
        """
        chain = []
        next_id: UUID | None = id
        while next_id is not None and self._held(next_id) is None:
            found = read.get(next_id) or self._read_manifest(next_id)
            if found is None:
                msg = f"No revision {next_id} in {self.uri}"
                raise KeyError(msg)
            chain.append(found)
            next_id = found.parent
        self._cache(reversed(chain))

    def revision(self, id: UUID) -> Revision:
        """Look one revision up, reading its manifest and any parents' not yet held.

        Raises
        ------
        KeyError
            If this store has no revision `id`, a pending layer included.

        Notes
        -----
        - [the layers](https://energy-models.github.io/datarecord/design/storage/store/#the-layers)
        """
        self._cache_upward(id, {})
        return Revision(id=id, store=self)

    def _scan(self) -> set[UUID]:
        """List `layers/` once, cache each manifest not yet held, and return the pending ids.

        A pending layer is one listed without a manifest.
        """
        held = {
            row[0]
            for row in self.workspace.con.execute(
                "SELECT id FROM revisions WHERE location = ?", [self.uri]
            ).fetchall()
        }
        listed = {UUID(name) for name in self.files.list(f"{self.uri}/layers/")}
        read = {id: self._read_manifest(id) for id in listed - held}
        for id, manifest in read.items():
            if manifest is not None:
                self._cache_upward(id, read)
        return {id for id, m in read.items() if m is None}

    def revisions(self, *, meta: Meta | None = None) -> list[Revision]:
        """Every revision in this store whose `meta` holds each pair of `meta`, unordered.

        A pending layer is none. One listing of `layers/`, reading only the
        manifests the workspace does not already hold; a value matches only
        one of its own type, so `1` matches neither `1.0` nor `True`.

        Raises
        ------
        pydantic.ValidationError
            If `meta` is not flat, each value a string, finite number or boolean.

        Notes
        -----
        - [the layers](https://energy-models.github.io/datarecord/design/storage/store/#the-layers)
        """
        wanted = json.dumps(validate_meta(meta))
        self._scan()
        rows = self.workspace.con.execute(
            "SELECT id FROM revisions WHERE location = ? AND json_contains(meta, ?)",
            [self.uri, wanted],
        ).fetchall()
        return [Revision(id=id, store=self) for (id,) in rows]

    def _read_claim(self, id: UUID) -> ClaimFile:
        """Layer `id`'s claim file.

        Raises
        ------
        KeyError
            If layer `id` has no claim.
        """
        claimed = ClaimFile.read(self.files, self.claim_uri(id))
        if claimed is None:
            msg = f"No claim {id} in {self.uri}"
            raise KeyError(msg)
        return claimed

    def _claimed(self) -> list[ClaimFile]:
        """The claim file of each pending layer, skipping a directory that holds none.

        Such a directory is a claim still being written, or what a cleanup
        under a running commit left behind.
        """
        read = (ClaimFile.read(self.files, self.claim_uri(id)) for id in self._scan())
        return [c for c in read if c is not None]

    def claims(self) -> list[Claim]:
        """Every pending layer in this store, claimed and not committed, oldest first.

        Read from the claim files on each call and never cached, since a
        pending layer may commit at any time.

        Notes
        -----
        - [cleanup](https://energy-models.github.io/datarecord/design/storage/store/#cleanup)
        """
        claims = [Claim(store=self, **asdict(c)) for c in self._claimed()]
        return sorted(claims, key=lambda c: c.created)

    def cleanup(
        self,
        *,
        ids: Iterable[UUID] | None = None,
        older_than: timedelta | None = None,
    ) -> list[UUID]:
        """Delete pending layers: those in `ids`, or those claimed `older_than` ago or earlier.

        Only a pending layer is deleted: each layer's manifest is rechecked
        right before its delete, without a lock. A job may hold its claim for
        days, so nothing deletes but this call, and a layer whose job still
        runs must be neither named nor young enough to match.

        Returns
        -------
        list of UUID
            The ids of the layers deleted.

        Raises
        ------
        TypeError
            Unless exactly one of `ids` and `older_than` is given.
        ValueError
            Naming each of `ids` that is no pending layer, before anything is
            deleted.

        Notes
        -----
        - [cleanup](https://energy-models.github.io/datarecord/design/storage/store/#cleanup)
        """
        match ids, older_than:
            case None, timedelta():
                cutoff = datetime.now(UTC) - older_than
                doomed = [c.id for c in self._claimed() if c.created <= cutoff]
            case Iterable(), None:
                doomed = list(dict.fromkeys(ids))
                pending = self._scan()
                refused = [str(i) for i in doomed if i not in pending]
                if refused:
                    msg = (
                        f"{refused} are no pending layers of {self.uri}; cleanup "
                        f"deletes only a layer claimed and not committed"
                    )
                    raise ValueError(msg)
            case _:
                msg = "`cleanup` takes exactly one of `ids=` and `older_than=`"
                raise TypeError(msg)
        deleted = []
        for id in doomed:
            if self.files.exists(self.manifest_uri(id)):
                continue
            self.files.delete(self.layer_dir(id))
            deleted.append(id)
        return deleted

    def layer(self, id: UUID) -> Record:
        """Revision `id`'s layer read alone, as the one layer it is.

        Under its own manifest, as `Workspace.at` reads a standalone directory.
        Not the revision's record - nothing beneath it is folded in.

        Raises
        ------
        KeyError
            If this store has no revision `id`.
        """
        manifest = self.revision(id).manifest
        # Not `id`, whose fold tables hold the whole ancestry's fold.
        return _one_layer(
            self.layer_dir(id), manifest, self.workspace, uuid5(id, "layer")
        )


class _OfStore(BaseModel):
    """A layer of a store, one location and one id whatever it has read.

    Serialises as them, and validates back through `Workspace.process()`.

    Notes
    -----
    - [crossing a process boundary](https://energy-models.github.io/datarecord/design/storage/concurrency/#crossing-a-process-boundary)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: UUID
    store: Annotated[Store, PlainSerializer(lambda s: s.uri, return_type=str)]

    @model_validator(mode="before")
    @classmethod
    def _attach(cls, data: Any) -> Any:
        """A location and id from the serialised form, completed by `_complete`."""
        if isinstance(data, dict) and isinstance(data.get("store"), str):
            store = Workspace.process().store(data["store"])
            data = cls._complete(store, UUID(str(data["id"])))
        return data

    @classmethod
    def _complete(cls, store: Store, id: UUID) -> dict[str, Any]:
        """The fields of layer `id` of `store`, read from its files."""
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.store.uri, self.id) == (other.store.uri, other.id)

    def __hash__(self) -> int:
        return hash((self.store.uri, self.id))


class Claim(_OfStore):
    """A layer id handed out by `Store.claim`, whose layer is pending until `commit`.

    Serialises as its store's location and its id, and validates or unpickles
    back through `Workspace.process()` by reading its claim file, so a job in
    another process commits it.

    Attributes
    ----------
    parent
        The revision the layer will fold over, `None` for a root.
    meta
        The revision's meta as claimed, which the commit keeps.
    created
        When the id was handed out.

    Notes
    -----
    - [a revision exists only by its file](https://energy-models.github.io/datarecord/design/storage/store/#a-revision-exists-only-by-its-file)
    - [crossing a process boundary](https://energy-models.github.io/datarecord/design/storage/concurrency/#crossing-a-process-boundary)
    """

    parent: UUID | None = Field(exclude=True)
    meta: Meta = Field(exclude=True)
    created: datetime = Field(exclude=True)

    @classmethod
    def _complete(cls, store: Store, id: UUID) -> dict[str, Any]:
        """Completed from the claim file.

        Raises
        ------
        KeyError
            If the layer has no claim, as after a cleanup.
        """
        return {"store": store, **asdict(store._read_claim(id))}

    def __reduce__(self) -> tuple[Any, ...]:
        return Claim.model_validate_json, (self.model_dump_json(),)

    def _refuse_cleaned_up(self) -> None:
        """Raise once `Store.cleanup` deleted this claim's layer.

        Checked before the data and again right before the manifest, which
        narrows but cannot close the window a concurrent cleanup races in.

        Raises
        ------
        ValueError
            If the claim file is gone.
        """
        if not self.store.files.exists(self.store.claim_uri(self.id)):
            msg = (
                f"layer {self.id} was cleaned up, so its claim commits no "
                f"more; claim a new layer"
            )
            raise ValueError(msg)

    def commit(
        self, source: LayerData | RecordLike, *, meta: Meta | None = None
    ) -> Revision:
        """Write `source` as this claim's layer, then its manifest, which makes it a revision.

        The data goes straight into the layer directory; the manifest, written
        create-only and last, lists its files, its full schema, the claim's
        parent and the claim's `meta` with `meta` added. A `WorkingRecord` is
        written as its edits.

        Raises
        ------
        ValueError
            If `meta` changes a key the claim set, `source` is edits staged
            over another revision than the claim's parent, the source's schema
            would make the parent's layers unreadable, the layer is committed
            already, or `source` fails validation. The layer is left pending.
        ValueError
            If `Store.cleanup` deleted the layer, before or while its data was
            written.
        PreconditionFailedError
            If another commit of this claim wrote the manifest first.
        KeyError
            If the claim's parent is gone from the store.
        pydantic.ValidationError
            If `meta` is not flat, each value a string, finite number or boolean.

        Notes
        -----
        - [a revision exists only by its file](https://energy-models.github.io/datarecord/design/storage/store/#a-revision-exists-only-by-its-file)
        """
        store = self.store
        meta = merge_meta(self.meta, validate_meta(meta))
        if isinstance(source, WorkingRecord):
            source = source.edits
        if isinstance(source, StagedSource) and source.base != self.parent:
            msg = (
                f"these edits were staged over revision {source.base}, so they "
                f"are a delta against it and not against {self.parent}; commit "
                f"them as a child of the revision the `WorkingRecord` was built over"
            )
            raise ValueError(msg)
        if store.files.exists(store.manifest_uri(self.id)):
            msg = f"revision {self.id} is committed already; a claim commits once"
            raise ValueError(msg)
        self._refuse_cleaned_up()
        parent = None if self.parent is None else store.revision(self.parent)
        data = layer_data(source, store.workspace.con)
        schema = data.schema
        if parent is not None:
            problems = schema.compatible_with(parent.manifest.schema)
            if problems:
                msg = (
                    f"this layer's schema is incompatible with its parent's: "
                    f"{'; '.join(problems)} (https://energy-models.github.io/datarecord/design/model/schema/#versioning)"
                )
                raise ValueError(msg)
        sources = [] if parent is None else parent.record.sources
        files = write_layer(
            data, store.layer_dir(self.id), store.workspace, parents=sources
        )
        manifest = Manifest(
            id=self.id, parent=self.parent, schema=schema, files=files, meta=meta
        )
        self._refuse_cleaned_up()
        manifest.write(store.files, store.manifest_uri(self.id))
        store._cache([manifest])
        return Revision(id=self.id, store=store)


_MANIFESTS = """
SELECT r.id, r.parent, r.meta, r.files, s.schema
FROM revisions r JOIN schemas s ON s.hash = r.schema
"""

_ANCESTORS = """
WITH RECURSIVE ancestors(id, parent, depth) AS (
  SELECT id, parent, 0 FROM revisions WHERE location = $location AND id = $id
  UNION ALL
  SELECT d.id, d.parent, ancestors.depth + 1
  FROM revisions d
  JOIN ancestors ON d.id = ancestors.parent
  WHERE d.location = $location
)
"""

_LINEAGE = (
    _ANCESTORS
    + _MANIFESTS
    + """
JOIN ancestors a ON a.id = r.id
WHERE r.location = $location
ORDER BY a.depth DESC
"""
)


def ancestry(store: Store, revision_id: UUID) -> list[UUID]:
    """Revision ids along the root->node path, root first - resolution order.

    The whole path. Truncating it at the nearest materialised node is the
    reader's business (`cache.sources_to_read`), since whether a node's
    caches exist is a fact about the filesystem rather than about the tree.

    Raises
    ------
    RuntimeError
        As `lineage` does.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    return [manifest.id for manifest in lineage(store, revision_id)]


def lineage(store: Store, revision_id: UUID) -> list[Manifest]:
    """The manifests along the root->node path, root first, as `ancestry` orders them.

    Raises
    ------
    RuntimeError
        If the workspace holds an ancestor without its parent, which would
        cut the path short.
    """
    store.revision(revision_id)
    rows = store.workspace.con.execute(
        _LINEAGE, {"location": store.uri, "id": revision_id}
    ).fetchall()
    manifests = _manifests(rows)
    first = manifests[0]
    if first.parent is not None:
        msg = (
            f"the workspace holds revision {first.id} of {store.uri} without its "
            f"parent {first.parent}; delete the workspace's database, which "
            f"holds only caches"
        )
        raise RuntimeError(msg)
    return manifests


def _manifests(rows: list[tuple[Any, ...]]) -> list[Manifest]:
    """`_MANIFESTS` rows as manifests, each distinct schema parsed once."""
    schemas: dict[str, Schema] = {}
    manifests = []
    for id, parent, meta, files, text in rows:
        if text not in schemas:
            schemas[text] = Schema.model_validate_json(text)
        manifests.append(
            Manifest(
                id=id,
                parent=parent,
                schema=schemas[text],
                files=LayerFiles(**files),
                meta=json.loads(meta),
            )
        )
    return manifests


class Revision(_OfStore):
    """A node in the tree of layers.

    Each revision adds one parquet layer on top of its parent's; the record it
    resolves to is that layer over its ancestors'. The layer location derives
    from `id` via its store's `layer_dir`, it is not stored.

    Serialises its store as the location, and validates or unpickles back
    through `Workspace.process()`, reading the manifest and every parent's
    the process does not yet hold - so `ancestry()` answers in a fresh process.
    Nothing cached travels.

    Notes
    -----
    - [layered resolution](https://energy-models.github.io/datarecord/design/layers/tree/)
    - [crossing a process boundary](https://energy-models.github.io/datarecord/design/storage/concurrency/#crossing-a-process-boundary)
    """

    _record: Record | None = PrivateAttr(default=None)
    _manifest: Manifest | None = PrivateAttr(default=None)

    @classmethod
    def _complete(cls, store: Store, id: UUID) -> dict[str, Any]:
        """Read with every parent's manifest the process does not yet hold.

        Raises
        ------
        KeyError
            If the store has no revision `id`.
        """
        store.revision(id)
        return {"id": id, "store": store}

    def __reduce__(self) -> tuple[Any, ...]:
        return self.store.revision, (self.id,)

    @property
    def record(self) -> Record:
        """This revision's resolved view, as a `Record` built once from its ancestry.

        The ancestry is truncated at the deepest materialised ancestor, so a
        deep tree resolves from few entries. Cached rather than rebuilt per
        call: layers are write-once, so the only thing that can change
        the truncation point is `materialise`, which clears this itself. The
        `Record` holds no cursor, so the cache serves every thread.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        if self._record is None:
            manifests = lineage(self.store, self.id)
            sources = sources_to_read(manifests, self.store)
            self._record = Record(self.id, sources, self.store.workspace)
        return self._record

    @property
    def manifest(self) -> Manifest:
        """This revision's manifest: its parent, schema, files and `meta`.

        Notes
        -----
        - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
        """
        if self._manifest is None:
            self.store.revision(self.id)
            rows = self.store.workspace.con.execute(
                _MANIFESTS + "WHERE r.location = ? AND r.id = ?",
                [self.store.uri, self.id],
            ).fetchall()
            self._manifest = _manifests(rows)[0]
        return self._manifest

    @property
    def parent(self) -> UUID | None:
        """This revision's parent's id, as its manifest holds it."""
        return self.manifest.parent

    @property
    def meta(self) -> Meta:
        """This revision's own `meta`, as its manifest holds it."""
        return self.manifest.meta

    # -- tree ---------------------------------------------------------------

    def materialise(self) -> None:
        """Write this node's caches - owner map, dim owners and dead labels.

        A policy rather than a lifecycle step, and purely additive: it changes
        no answer, only how many layers a descendant's read touches. Once these
        exist, a read stops here instead of walking further up.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        materialise(self.id, self.record.sources, self.store)
        self._record = None

    # -- read ---------------------------------------------------------------

    def ancestry(self) -> list[UUID]:
        """Revision ids along the root->self path, root first.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        return ancestry(self.store, self.id)


def _absolute(uri: str) -> str:
    """`uri` without a trailing slash, a local path resolved against the cwd."""
    return Path(uri).resolve().as_posix() if "://" not in uri else uri.rstrip("/")


def _one_layer(
    base: str, manifest: Manifest, workspace: Workspace, layer_id: UUID
) -> Record:
    """The layer at `base` read alone under `manifest`, as `layer_id`.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
    """
    source = DirectorySource(base, manifest, workspace, layer_id)
    return Record(layer_id, [source], workspace)


def _process_store(uri: str) -> Store:
    """`uri` as a store of the process workspace, which is what unpickling needs."""
    return Workspace.process().store(uri)
