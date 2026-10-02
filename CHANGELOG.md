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
  relations and parameters. It needs the `mathspec` extra:
  `pip install 'datarecord[mathspec]'`.
- `datarecord.sources`: `from_sources` reads tables keyed by the names a schema
  declares as a record, and `to_sources` returns a record as those tables, in
  the shape specsolve's `solve` takes.

### Changed

- The core names no dim. Any dim in `partial` can have labels added and
  removed, a NULL broadcasts over the dims an attribute names, and `set`,
  `add(dim, frame)` and `flags(**labels)` take any dim.
- A component's type is an ordinary group, `groups/entity_type.parquet`, and
  removing a label removes the group rows keyed on it.
- Groups are relations: `Relation(key, values)`, `Schema.relations`,
  `record.relations` and `add_relation`. A relation's rows are stored under
  `relations/`, and attribute values under `attributes/`.
- An attribute is over dims only. Data on a relation's rows goes over a dim of
  its own, related to the relation's columns.
- A dim declares `ordered` where its labels' order is part of the data.
  `Schema.from_mathspec` reads it from mathspec and, unless the storage block
  names `partial`, makes every dim not declared `ordered` partial. A relation
  may be keyed by a dim outside `partial`.
- An attribute over one dim is stored like any other attribute, as rows of
  `attributes/<attr>.parquet`. An axis file holds its labels only, so
  `dims[dim]` carries no attribute column, and `attributes` and `flags` answer
  for every attribute.
- `set` takes a scalar, a long frame or an `nw.Expr`. A different value per
  label is a frame with a column per coordinate and a `value` column; a
  mapping, a sequence or a series is refused with a `TypeError` that spells the
  frame, and `indexed_by` is gone.

### Fixed

- `set`, `remove` and `remove_relation` refuse a label whose type is not its
  dim's declared dtype with a `TypeError` that names the dim, its dtype, the
  label and the rewrite, where they failed inside pyarrow or DuckDB. A str is
  no longer parsed as a `Datetime` label: pass `pd.Timestamp("2030-01-01")`.
- A layer that holds a default and its exception side by side, such as
  `set("efficiency", 0.9, port=["dc_out"])` and `set("efficiency", 0.95)`,
  reads one value at each coordinate: the row that names more of the
  attribute's dims, and `flags` describes those rows. A write is refused where
  two rows cover one coordinate and neither names more dims than the other,
  or where two rows leave the same dims NULL at one coordinate, a scalar beside
  a curve included.

### Removed

- Traits, per-type member files, `Record.entity_types`, `Schema.attributes_for`
  and the check that names are unique across types.
- `datarecord.tools`, with the `Tool` protocol and the PyPSA tool, and the
  `pypsa` extra. A converter outside datarecord produces the tables that
  `from_sources` reads.
- Outputs: the `outputs/` directory, `Schema.results`, `Record.outputs` and
  `set(kind="outputs")`. A solve's answers are stored as a record of their own,
  whose schema the producer defines.

### Fixed

- `write_record` refuses a relation frame that carries a column beyond the
  relation's columns and its tombstone, as `add_relation` already does.
- `set(attribute, expr)` with a narwhals expression derives an attribute over
  one dim alone, such as `p_nom` over `entity`, from that dim's axis. It raised
  `KeyError` before (#33).
- `set(attribute, expr)` with a narwhals expression raises `KeyError` when any
  label it names has no current value, and names those labels. It derived the
  labels that had a value and skipped the others without a word, and raised
  only where none had one.
- Removing a label also removes the relation rows whose `values` column holds
  it. A removed entity no longer leaves `port_entity` rows that map its ports to
  it.
