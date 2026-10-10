<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- A `Workspace` holds the DuckDB connection and a `Store` one location;
  `connect`, `layer_dir`, `DATARECORD_BASE_URI`, `Record.at` and the process
  default connection are gone. A store's revisions live beside its layers as
  `revisions/<id>.json`, so a `Revision` serialises as its location and reads
  its ancestry back in a new process. `ws.con` is the calling thread's cursor,
  and a relation from another connection or thread is rejected at the write
  and edit boundary.
- Writing is `store.root(source)`, `rev.child(source)` and
  `ws.write(source, uri)`; `write_record`, `Revision.create` and the
  source-less `Revision.child()` are gone. A `WorkingRecord` no longer
  commits: `rev.child(w.edits)` writes its edits as a child,
  `ws.write(w, uri)` the flattened record, and `NewChild`, `Directory` and
  `commit` are gone. A tree write writes its revision file last, so a failed write leaves
  no revision and never widens the manifest.
- `Tool.to_datarecord(model, con)` and `Tool.results(model, con)` take the
  connection their frames are planned on, so `PyPSA.results(n, w.con)` sets
  straight onto `w` rather than failing on a relation from DuckDB's default
  connection; `on_connection` is gone.

### Added

- A store or standalone directory on `s3://`, through boto3 behind the
  `datarecord[remote]` extra and configured by the AWS environment alone.
  Revision files are written create-only and the manifest by
  compare-and-swap, so two writers widening one schema never silently lose
  a widening.

### Fixed

- `materialise()` writes `owner_map/inputs.parquet` last, each file through
  a temporary name, so a node never reads as materialised from half its
  cache.
- A fold cached before the manifest widened now reads under the wider schema.
- Reading an attribute over a `partial` dim no longer slows with the square
  of that dim's length.
- Folding the owner map no longer compares every row with every label of a
  `partial` dim's axis; the first read of a record with long series is linear
  again.
