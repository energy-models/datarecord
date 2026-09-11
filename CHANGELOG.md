<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- A schema declaring a dim, group, attribute or result named after a column the
  format writes for itself is refused at load, rather than colliding with it
  silently. The set is `datarecord.schema.RESERVED`, listed in
  [the record format](https://energy-models.github.io/datarecord/design/format/#reserved-column-names).
