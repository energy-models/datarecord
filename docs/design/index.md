<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Overview

Status: Draft · Owner: Jonas Hörsch · Date: 2026-08-05

A record holds labeled data with a declared schema. Its labels sit on axes called dims. A multi-dim is a dim whose labels are tuples of other dims' labels, listing which combinations exist. Properties describe single labels, and attributes hold values at coordinates. The schema declares what may exist, and the data says what does. A record can be one parquet directory or a tree of layers, and a reader reads both through one interface. The package depends on DuckDB, pydantic and mathspec, and names no modelling framework.

These pages are the authoritative design. [Usage](../usage/index.md) shows how to use the package. Docstrings in the source cite these pages by link.

## The parts of a record

A [dim](model/dims.md) is an axis: a set of unique labels, such as the buses of a network or its snapshots. A dim with `one_of` fixes which labels it may hold.

A [multi-dim](model/multi-dims.md) is a dim whose labels are tuples of other dims' labels. It declares which tuples exist, such as which entity attaches to which bus.

A [property](model/properties.md) holds one value per label of a dim and describes that label, such as the carrier of a generator. A property can refer back to another dimension using `one_of`.

An [attribute](model/attributes.md) holds values at coordinates over the dims it names, such as a generator's available output per snapshot, or the resulting dispatch of the solution.

The [schema](model/schema.md) declares the dims, properties and attributes with their dtypes. The data says which labels, tuples and values exist.

A `RecordLike` object exposes each of them, and returns dims, properties and attributes as DuckDB relations. The [reading](use/reading.md) page defines the protocol in full.

```text
record.schema                   what may exist: the axes, properties, attributes
record.dims                     the axes themselves, multi-dims included, keyed by dim
record.properties               what describes each label, keyed by dim
record.attributes               the values, results included, keyed by attribute name
record.flags(**cats)            which axes an attribute actually uses
```

## The running example

The pages illustrate the design with one example schema, shaped like PyPSA's ([the PyPSA tool](tools/pypsa.md)). Its dims are `entity`, `bus`, `carrier`, `snapshot` and `ctype`.
The properties `entity.ctype` and `entity.carrier` refer back to their dimension. `ctype` is a dim with `one_of`, whose labels are component classes such as `Generator`, `Link` and `Load`, so an attribute over `entity` can have a default per `ctype`. The multi-dim `connection` lists the `(entity, bus)` tuples that exist, and attributes such as `p_nom` and `p_max_pu` hold values over `entity` and `snapshot`. The schema declares each of these names like any other.

## Core interface

`Record` is the one class that implements the protocol. It reads a fold across layers:

- A tree of layers, each adding a partial record on top of its parent, a deeper layer's row winning where it crosses a shallower one. `Revision.record` gives one ([a tree of layers](layers/tree.md)).
- One parquet directory, through `Workspace.at(uri)`. It folds as a tree with one layer, which [degenerates to a scan of it](layers/resolution.md#one-record-over-one-fold).
- A [`WorkingRecord`](use/editing.md), whose last layer stages edits, so pending edits read back before anything is written.

A framework's own object may satisfy `RecordLike` too, such as a PyPSA `Network` that presents itself as a record and depends on nothing in this package. A consumer cannot tell which kind it holds, so a framework reads a hundred-layer tree through the same calls as a single directory.

A framework consumes a record, a workflow engine produces one, and neither needs to know how the other works. `datarecord` depends on `duckdb`, `pydantic` and `mathspec`, whose dtypes a schema declares. Its optional `datarecord[remote]` adds boto3 for a location on S3 ([files, local and remote](storage/store.md#files-local-and-remote)). A framework's tool lives under `datarecord/tools/` behind its own extra ([the package](package.md)).

## Scope

The design covers the `RecordLike` protocol and `WorkingRecord`, the data model and its schema, layers and their resolution, the write path, and the parquet format that stores a record.

Out of scope are an implementation on an engine other than DuckDB ([one engine](use/reading.md#one-engine)) and layers that are not materialised as parquet. Concurrent writers to one record meet only at the manifest ([concurrent writers](storage/concurrency.md)).

## The pages

| section                             | what it settles                                                            |
| ----------------------------------- | -------------------------------------------------------------------------- |
| [The data model](model/dims.md)     | dims, multi-dims, properties, attributes and the schema that declares them |
| [Layers](layers/tree.md)            | the tree of layers, removing labels, and how a read resolves               |
| [Using a record](use/reading.md)    | reading, editing with `WorkingRecord`, and writing a layer                 |
| [Storage](storage/format.md)        | the parquet format, workspaces and stores, threads and processes           |
| [Frameworks](tools/index.md)        | tools, and the PyPSA tool                                                  |
| [The package](package.md)           | where the code lives, and the one-way dependency                           |
| [Open questions](open-questions.md) | what is deliberately unsettled                                             |
