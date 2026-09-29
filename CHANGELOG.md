<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `Schema.from_declarations` builds a schema from mathspec declarations, and
  `Schema.to_declarations` writes one back.

### Changed

- The core names no dim. Any dim in `partial` can have labels added and
  removed, a NULL broadcasts over the dims an attribute names, and `set`,
  `add(dim, frame)` and `flags(**labels)` take any dim. `partial` must name
  every dim a group is keyed by.
- A component's type is an ordinary group, `groups/entity_type.parquet`, and
  removing a label removes the group rows keyed on it.

### Removed

- Traits, per-type member files, `Record.entity_types`, `Schema.attributes_for`
  and the check that names are unique across types.
