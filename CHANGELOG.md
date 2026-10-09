<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Zip records: `write_record(..., uri=..., zip=True)` writes a record as an
  uncompressed zip archive, and `Record.at("zip://archive.zip/")` reads it.

### Fixed

- Reading an attribute over a `partial` dim no longer slows with the square
  of that dim's length.
