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

### Fixed

- A fold cached before the manifest widened now reads under the wider schema.

- Reading an attribute over a `partial` dim no longer slows with the square
  of that dim's length.
