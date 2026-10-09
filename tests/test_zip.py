# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A record packed into a zip archive, read back through the `zip://` scheme."""

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from datarecord.duck import read_json
from datarecord.layered.resolve import write_schema
from datarecord.layered.revision import Record
from datarecord.layered.write import write_record
from datarecord.schema import Schema
from datarecord.tools.pypsa import PyPSA


@pytest.fixture
def archive(con, base_uri, ac_dc, tmp_path):
    archive = tmp_path / "record.zip"
    write_record(None, PyPSA.to_datarecord(ac_dc), con, uri=str(archive), zip=True)
    return archive


def test_zip_record_matches_the_directory_form(con, base_uri, ac_dc, archive, tmp_path):
    directory = tmp_path / "record"
    write_record(None, PyPSA.to_datarecord(ac_dc), con, uri=str(directory))

    from_dir = Record.at(f"{directory}/", con)
    from_zip = Record.at(f"zip://{archive}/", con)

    assert set(from_zip.attributes) == set(from_dir.attributes)
    assert set(from_zip.entity_types) == set(from_dir.entity_types)
    got = from_zip.attributes["p_max_pu"].collect().sort("entity").to_pandas()
    want = from_dir.attributes["p_max_pu"].collect().sort("entity").to_pandas()
    pd.testing.assert_frame_equal(got, want)


def test_every_entry_is_stored_uncompressed(archive):
    with zipfile.ZipFile(archive) as zf:
        methods = {info.compress_type for info in zf.infolist()}
    assert methods == {zipfile.ZIP_STORED}, "deflated entries cannot be range-read"


def test_read_json_reads_a_zip_entry(archive, con):
    manifest = read_json(f"zip://{archive}/manifest.json", con)
    assert manifest is not None
    assert "attributes" in manifest
    assert read_json(f"zip://{archive}/nonesuch.json", con) is None


def test_read_json_needs_a_connection_for_a_remote_uri():
    with pytest.raises(ValueError, match="connection"):
        read_json("s3://bucket/manifest.json")


def test_writing_a_manifest_to_a_remote_base_is_refused():
    with pytest.raises(NotImplementedError, match="remote"):
        write_schema(Schema(), "s3://bucket/record")


def test_zip_write_refuses_an_existing_archive(con, ac_dc, archive):
    with pytest.raises(FileExistsError):
        write_record(None, PyPSA.to_datarecord(ac_dc), con, uri=str(archive), zip=True)


@pytest.mark.parametrize("uri", [None, "s3://bucket/r.zip"])
def test_zip_write_needs_a_local_uri(con, base_uri, ac_dc, uri):
    with pytest.raises(ValueError, match="local uri"):
        write_record(None, PyPSA.to_datarecord(ac_dc), con, uri=uri, zip=True)


def test_a_failed_pack_leaves_no_archive(con, base_uri, ac_dc, tmp_path, monkeypatch):
    archive = tmp_path / "record.zip"
    real_write = zipfile.ZipFile.write

    def boom(self, *args, **kwargs):
        real_write(self, *args, **kwargs)
        raise RuntimeError("pack interrupted")

    monkeypatch.setattr(zipfile.ZipFile, "write", boom)
    with pytest.raises(RuntimeError):
        write_record(None, PyPSA.to_datarecord(ac_dc), con, uri=str(archive), zip=True)
    assert not archive.exists(), "no archive"
    assert not Path(f"{archive}.staging").exists(), "no staging leftover"
