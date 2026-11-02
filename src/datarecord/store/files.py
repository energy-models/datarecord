# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The small files a store coordinates through, local or remote.

Tables go through DuckDB; everything else - the manifests, the claims,
listing them, whether a layer exists - goes through a `Files`, picked by the
URI's scheme.

Notes
-----
- [files, local and remote](https://energy-models.github.io/datarecord/design/storage/store/#files-local-and-remote)
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Protocol


class Absent:
    """The precondition that nothing is at the URI yet: a create-only write."""


class PreconditionFailedError(Exception):
    """A conditional write found the object other than `expect` said."""


class Files(Protocol):
    """Reading, writing and listing small files under one kind of URI.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/design/storage/store/#files-local-and-remote)
    """

    def read_text(self, uri: str) -> str | None:
        """The text at `uri`, or `None` on a miss."""
        ...

    def write_text(self, uri: str, text: str, *, expect: Absent | None) -> None:
        """Write `text` to `uri`, if `expect` holds.

        Parameters
        ----------
        expect
            `Absent` writes only if nothing is there, `None` unconditionally.

        Raises
        ------
        PreconditionFailedError
            If `expect` does not hold.
        """
        ...

    def list(self, prefix: str) -> list[str]:
        """The names directly under `prefix`, empty where there is nothing."""
        ...

    def exists(self, uri: str) -> bool: ...

    def delete(self, prefix: str) -> None:
        """Delete everything under `prefix`, which ends in a slash.

        Raises
        ------
        FileNotFoundError
            If nothing is under `prefix`.
        """
        ...


class LocalFiles:
    """`Files` on the local filesystem.

    Every write lands under a temporary name and is renamed or linked into
    place, so a reader sees a whole file or none.
    """

    def read_text(self, uri: str) -> str | None:
        try:
            return Path(uri).read_text()
        except FileNotFoundError:
            return None

    def write_text(self, uri: str, text: str, *, expect: Absent | None) -> None:
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(text)
            if isinstance(expect, Absent):
                try:
                    os.link(tmp, path)
                except FileExistsError:
                    msg = f"{uri} already exists"
                    raise PreconditionFailedError(msg) from None
            else:
                os.replace(tmp, path)
        finally:
            Path(tmp).unlink(missing_ok=True)

    def list(self, prefix: str) -> list[str]:
        path = Path(prefix)
        if not path.is_dir():
            return []
        return [p.name for p in path.iterdir()]

    def exists(self, uri: str) -> bool:
        return Path(uri).exists()

    def delete(self, prefix: str) -> None:
        shutil.rmtree(prefix)


def _split(uri: str) -> tuple[str, str]:
    """`s3://bucket/key` as `(bucket, key)`."""
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


class RemoteFiles:
    """`Files` on S3, through boto3, configured by the AWS environment alone.

    `Absent` maps to `put_object`'s conditional header `IfNoneMatch="*"`,
    which holds only as far as the server honours it.

    Raises
    ------
    ImportError
        If boto3 is not installed: it comes with `datarecord[remote]`.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/design/storage/store/#files-local-and-remote)
    """

    def __init__(self) -> None:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as e:
            msg = (
                "an s3:// URI needs boto3; install it with "
                "`pip install datarecord[remote]`"
            )
            raise ImportError(msg) from e
        # A self-hosted endpoint (SeaweedFS, MinIO) has no bucket-subdomain DNS.
        style = "path" if os.environ.get("AWS_ENDPOINT_URL") else "auto"
        self._s3 = boto3.client("s3", config=Config(s3={"addressing_style": style}))
        self._error = self._s3.exceptions.ClientError

    @staticmethod
    def _status(e: Any) -> int:
        return e.response["ResponseMetadata"]["HTTPStatusCode"]

    def read_text(self, uri: str) -> str | None:
        bucket, key = _split(uri)
        try:
            got = self._s3.get_object(Bucket=bucket, Key=key)
        except self._error as e:
            # 403 too: S3 answers a missing key so without ListBucket.
            if self._status(e) in (403, 404):
                return None
            raise
        return got["Body"].read().decode()

    def write_text(self, uri: str, text: str, *, expect: Absent | None) -> None:
        bucket, key = _split(uri)
        condition = {"IfNoneMatch": "*"} if isinstance(expect, Absent) else {}
        try:
            self._s3.put_object(Bucket=bucket, Key=key, Body=text.encode(), **condition)
        except self._error as e:
            # 409 is a conditional write racing another on the same key.
            if self._status(e) in (409, 412):
                msg = f"{uri} is not as `expect` said: {e}"
                raise PreconditionFailedError(msg) from e
            raise

    def list(self, prefix: str) -> list[str]:
        bucket, key = _split(prefix)
        if key and not key.endswith("/"):
            key += "/"
        names = []
        pages = self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=key, Delimiter="/"
        )
        for page in pages:
            names += [o["Key"][len(key) :] for o in page.get("Contents", [])]
            names += [
                p["Prefix"][len(key) :].rstrip("/")
                for p in page.get("CommonPrefixes", [])
            ]
        return [n for n in names if n]

    def exists(self, uri: str) -> bool:
        """An object at `uri`, or any under it, as a local directory exists."""
        bucket, key = _split(uri)
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except self._error as e:
            if self._status(e) not in (403, 404):
                raise
        listed = self._s3.list_objects_v2(
            Bucket=bucket, Prefix=key.rstrip("/") + "/", MaxKeys=1
        )
        return listed.get("KeyCount", 0) > 0

    def delete(self, prefix: str) -> None:
        bucket, key = _split(prefix)
        pages = self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=key
        )
        keys = [{"Key": o["Key"]} for page in pages for o in page.get("Contents", [])]
        if not keys:
            msg = f"nothing under {prefix} to delete"
            raise FileNotFoundError(msg)
        for start in range(0, len(keys), 1000):
            batch = {"Objects": keys[start : start + 1000]}
            done = self._s3.delete_objects(Bucket=bucket, Delete=batch)
            if done.get("Errors"):
                msg = f"deleting under {prefix} failed: {done['Errors']}"
                raise OSError(msg)
