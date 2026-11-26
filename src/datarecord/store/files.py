# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The small files a store coordinates through, local or remote.

Tables go through DuckDB; everything else - the manifests, the claims,
listing them, whether a layer exists - goes through a `Files`, picked by the
URI's scheme.

Notes
-----
- [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit


class PreconditionFailedError(Exception):
    """A create-only write found something at its URI already."""


class Files(Protocol):
    """Reading, writing and listing small files under one kind of URI.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
    """

    def read_text(self, uri: str) -> str | None:
        """The text at `uri`, or `None` on a miss."""
        ...

    def create(self, uri: str, text: str) -> None:
        """Write `text` to `uri`, only if nothing is there yet.

        Raises
        ------
        PreconditionFailedError
            If something is at `uri` already.
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

    Every write lands under a temporary name and is hard-linked into place,
    which the OS refuses over an existing file, so a reader sees a whole file
    or none.
    """

    def read_text(self, uri: str) -> str | None:
        try:
            return Path(uri).read_text()
        except FileNotFoundError:
            return None

    def create(self, uri: str, text: str) -> None:
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(text)
            try:
                os.link(tmp, path)
            except FileExistsError:
                msg = f"{uri} already exists"
                raise PreconditionFailedError(msg) from None
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


@dataclass(frozen=True)
class S3Credentials:
    """The S3 key, and the server it is for, that a `Workspace` reaches `s3://` URIs with.

    The `repr` leaves out `secret` and `session_token`.

    Parameters
    ----------
    key_id
        The access key id.
    secret
        The secret access key.
    session_token
        The session token of a temporary key, such as one from STS.
    endpoint
        The server's URL, such as `http://localhost:8333`, or `None` for AWS.
    region
        The region the bucket is in.
    url_style
        `"path"` puts the bucket in the path, as a self-hosted server needs,
        and `"vhost"` in the host name.

    Raises
    ------
    ValueError
        If `key_id` or `secret` is empty, `endpoint` is no `http` or `https`
        URL without a user, path, query or fragment, or `url_style` is
        neither `"path"` nor `"vhost"`.

    Notes
    -----
    - [S3 credentials](https://energy-models.github.io/datarecord/reference/store/#s3-credentials)
    """

    key_id: str
    secret: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)
    endpoint: str | None = None
    region: str = "eu-central-1"
    url_style: Literal["path", "vhost"] = "vhost"

    def __post_init__(self) -> None:
        if not (self.key_id and self.secret):
            msg = "S3Credentials needs a non-empty `key_id` and `secret`"
            raise ValueError(msg)
        if self.endpoint is not None:
            url = urlsplit(self.endpoint)
            if (
                url.scheme not in ("http", "https")
                or not url.hostname
                or "@" in url.netloc
                or url.path not in ("", "/")
                or url.query
                or url.fragment
            ):
                msg = (
                    "`endpoint` takes the server's URL with its scheme and no "
                    "user, path, query or fragment, such as 'http://localhost:8333'"
                )
                raise ValueError(msg)
        if self.url_style not in ("path", "vhost"):
            msg = f"`url_style` is {self.url_style!r}; pass 'path' or 'vhost'"
            raise ValueError(msg)


def _split(uri: str) -> tuple[str, str]:
    """`s3://bucket/key` as `(bucket, key)`."""
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


class RemoteFiles:
    """`Files` on S3, through boto3, with `credentials` or else the AWS environment.

    The client has a boto3 session of its own, so it reads the AWS environment
    as it stands when built. With `credentials`, the key and the endpoint come
    from them, not from the environment. Without them, where the environment
    holds no key either, the client sends its requests unsigned, so it reaches
    what a bucket grants anyone.

    `create` is `put_object` with the conditional header `IfNoneMatch="*"`,
    which holds only as far as the server honours it.

    Raises
    ------
    ImportError
        If boto3 is not installed: it comes with `datarecord[remote]`.
    PermissionError
        From any method the server answers with 403, and from `delete` where
        every key it fails on is `AccessDenied`. Without ListBucket on the
        prefix, S3 answers 403 for an absent key too.

    Attributes
    ----------
    anonymous
        Whether the client sends its requests unsigned, for want of a key.

    Notes
    -----
    - [files, local and remote](https://energy-models.github.io/datarecord/reference/store/#files-local-and-remote)
    """

    def __init__(self, credentials: S3Credentials | None = None) -> None:
        try:
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config
        except ImportError as e:
            msg = (
                "an s3:// URI needs boto3; install it with "
                "`pip install datarecord[remote]`"
            )
            raise ImportError(msg) from e
        if credentials is None:
            # A self-hosted endpoint (SeaweedFS, MinIO) has no bucket-subdomain DNS.
            style = "path" if os.environ.get("AWS_ENDPOINT_URL") else "auto"
            session = boto3.Session()
            endpoint = None
            self.anonymous = session.get_credentials() is None
            config = Config(
                s3={"addressing_style": style},
                signature_version=UNSIGNED if self.anonymous else None,
            )
        else:
            session = boto3.Session(
                aws_access_key_id=credentials.key_id,
                aws_secret_access_key=credentials.secret,
                aws_session_token=credentials.session_token,
                region_name=credentials.region,
            )
            endpoint = credentials.endpoint
            self.anonymous = False
            style = {"path": "path", "vhost": "virtual"}[credentials.url_style]
            config = Config(
                s3={"addressing_style": style}, ignore_configured_endpoint_urls=True
            )
        self._s3 = session.client("s3", endpoint_url=endpoint, config=config)
        self._error = self._s3.exceptions.ClientError

    @staticmethod
    def _status(e: Any) -> int:
        return e.response["ResponseMetadata"]["HTTPStatusCode"]

    @contextmanager
    def _permitted(self, uri: str) -> Iterator[None]:
        """Raise a 403 from the S3 calls inside as a `PermissionError` naming `uri`."""
        try:
            yield
        except self._error as e:
            if self._status(e) == 403:
                msg = f"access to {uri} denied: {e}"
                raise PermissionError(msg) from e
            raise

    def read_text(self, uri: str) -> str | None:
        bucket, key = _split(uri)
        with self._permitted(uri):
            try:
                got = self._s3.get_object(Bucket=bucket, Key=key)
            except self._error as e:
                if self._status(e) == 404:
                    return None
                raise
        return got["Body"].read().decode()

    def create(self, uri: str, text: str) -> None:
        bucket, key = _split(uri)
        with self._permitted(uri):
            try:
                self._s3.put_object(
                    Bucket=bucket, Key=key, Body=text.encode(), IfNoneMatch="*"
                )
            except self._error as e:
                # 409 is a conditional write racing another on the same key.
                if self._status(e) in (409, 412):
                    msg = f"{uri} already exists: {e}"
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
        with self._permitted(prefix):
            for page in pages:
                names += [o["Key"][len(key) :] for o in page.get("Contents", [])]
                names += [
                    p["Prefix"][len(key) :].rstrip("/")
                    for p in page.get("CommonPrefixes", [])
                ]
        return [n for n in names if n]

    def exists(self, uri: str) -> bool:
        """An object at `uri`, or any under it, as a local directory exists.

        A `uri` ending in a slash is a directory, which exists by a key under
        it alone: SeaweedFS keeps a directory once its keys are deleted and
        answers a HEAD on it.
        """
        bucket, key = _split(uri)
        with self._permitted(uri):
            if not key.endswith("/"):
                try:
                    self._s3.head_object(Bucket=bucket, Key=key)
                    return True
                except self._error as e:
                    if self._status(e) != 404:
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
        with self._permitted(prefix):
            keys = [
                {"Key": o["Key"]} for page in pages for o in page.get("Contents", [])
            ]
            if not keys:
                msg = f"nothing under {prefix} to delete"
                raise FileNotFoundError(msg)
            for start in range(0, len(keys), 1000):
                batch = {"Objects": keys[start : start + 1000]}
                done = self._s3.delete_objects(Bucket=bucket, Delete=batch)
                errors = done.get("Errors", [])
                if errors and all(e["Code"] == "AccessDenied" for e in errors):
                    msg = f"deleting under {prefix} denied: {errors}"
                    raise PermissionError(msg)
                if errors:
                    msg = f"deleting under {prefix} failed: {errors}"
                    raise OSError(msg)
