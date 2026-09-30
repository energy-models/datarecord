<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `Schema.from_mathspec` builds a schema from a mathspec spec's dimensions,
  relations and parameters, and `Schema.to_mathspec` writes them back. Both
  need the `mathspec` extra: `pip install 'datarecord[mathspec]'`.
- `datarecord.sources`: `from_sources` reads tables keyed by the names a schema
  declares as a record, and `to_sources` returns a record as those tables, in
  the shape specsolve's `solve` takes.

### Changed

- The core names no dim. Any dim in `partial` can have labels added and
  removed, a NULL broadcasts over the dims an attribute names, and `set`,
  `add(dim, frame)` and `flags(**labels)` take any dim. `partial` must name
  every dim a group is keyed by.
- A component's type is an ordinary group, `groups/entity_type.parquet`, and
  removing a label removes the group rows keyed on it.
- Groups are relations: `Relation(key, values)`, `Schema.relations`,
  `record.relations` and `add_relation`. A relation's rows are stored under
  `relations/`, and attribute values under `attributes/`.
- An attribute is over dims only. Data on a relation's rows goes over a dim of
  its own, related to the relation's columns.

### Removed

- Traits, per-type member files, `Record.entity_types`, `Schema.attributes_for`
  and the check that names are unique across types.
- `datarecord.tools`, with the `Tool` protocol and the PyPSA tool, and the
  `pypsa` extra. A converter outside datarecord produces the tables that
  `from_sources` reads.
- Outputs: the `outputs/` directory, `Schema.results`, `Record.outputs` and
  `set(kind="outputs")`. A solve's answers are stored as a record of their own,
  whose schema the producer defines.
