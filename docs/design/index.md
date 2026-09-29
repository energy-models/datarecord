<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Design

Status: Draft · Owner: Jonas Hörsch · Date: 2026-08-05

The authoritative design for datarecord. [Usage](../usage/index.md) is how to
use the package; these pages are what it is and why it is that way. The
docstrings in the source cite them by link rather than restating the argument —
a comment that re-argues the design is a defect.

## What a data record is

Dimensioned attribute data with a declared schema.

A record holds **components** (named members of a type), **groups** of them — connections between components and buses being the one every network has — **attribute values** over both, and the **axes** those values vary along.
A schema declares what may exist; the data says what does.

A record exposes five things:

```text
record.schema           what may exist: the axes, the groups, the attributes
record.dims             the axes themselves, keyed by dim
record.groups           which tuples exist, keyed by group
record.attributes       the values, keyed by attribute name
record.flags(entities)  which axes an attribute actually uses
```

That is the [`Record` protocol](record.md), and [The Record protocol](record.md) gives it precisely.

A component's `entity` identifies it on its own: an entity is one label of [the entity axis](format.md#the-entity-axis), and its type is a row of a [group](schema.md#types).
That is why the values are keyed by attribute and not by type — an attribute row names a component and nothing more, and a component's type is something the record knows about it rather than part of its address.

`Record` is the one class that answers all of this, and it is the narwhals interface over a fold across layers:

- **A tree of layers**, each adding a partial record on top of its parent, resolved last-writer-wins. No single directory is the record; the answer is the fold across them. `Revision.record` gives one.
- **One parquet directory**, via `Record.at(uri)` — folded over the single layer it is, which [degenerates to a scan of it](read-path.md#one-record-over-one-fold). Not a second implementation.
- **[`WorkingRecord`](working-record.md)** — a `Record` whose last layer is a staging area, so pending edits read back before anything is written.

`RecordLike` is the protocol all of these satisfy, and so does the record [`from_sources`](sources.md) reads from **tables keyed by declared names**.

A consumer cannot tell which it holds, so a framework reads a hundred-layer overlay through the same call it would use for a single directory.

Neither the concept nor this package names a modelling framework.
A framework consumes a record, a workflow engine produces one, and neither needs to know how the other works.
`datarecord` depends only on `duckdb`, `narwhals` and `pydantic`.

## Scope

- **In scope:** the [`Record` protocol](record.md) (the definition) and [`WorkingRecord`](working-record.md); the [parquet format](format.md) that stores it; [the schema](schema.md); [overlay resolution](layers.md) and its [owner map](read-path.md#owner-map); [the write path](writing.md); [why one directory needs no second implementation](read-path.md#one-record-over-one-fold).
- **Out of scope:** a non-DuckDB implementation (the protocol permits one, see [the protocol names no engine](record.md#the-protocol-names-no-engine), but only DuckDB-backed ones are provided); concurrent writers to one record; unmaterialised/meta layers.

## The pages

| page                                  | what it settles                                       |
| ------------------------------------- | ----------------------------------------------------- |
| [The Record protocol](record.md)      | what a consumer codes against, and what it may assume |
| [The record format](format.md)        | the parquet directory a record is stored as           |
| [The schema](schema.md)               | what may exist: dims, attributes, patch granularity   |
| [Layered resolution](layers.md)       | a tree of layers, folded last-writer-wins             |
| [The DuckDB read path](read-path.md)  | the owner map and how a relation resolves             |
| [Writing a record](writing.md)        | `write_record`, and what it validates                 |
| [`WorkingRecord`](working-record.md)  | editing: staging, committing, reading back            |
| [Tables by declared name](sources.md) | the seam a solver or a framework meets                |
| [Module layout](module-layout.md)     | where the code lives, and the one-way dependency      |
| [Open questions](open-questions.md)   | what is deliberately unsettled                        |
