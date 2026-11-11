<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# One record

datarecord reads a directory and a tree of layers through one interface, so code that reads a record never depends on how it is stored.

## One interface over a directory and a tree

`RecordLike` is the interface a reader codes against ([the record](../reference/record.md)), and `Record` is the one class that implements it. A `Record` folds the layers of a revision, where each layer is the data one revision adds on top of its parent. A standalone directory folds as a tree of one layer. A `WorkingRecord` folds its pending edits as one more layer over its base, so the edits read back before anything is written.

The rejected alternative is a record class per form: one class for a directory, one for a revision and one for pending edits. Each would answer the same members by its own code, and every rule about rows would be written, tested and kept in step three times. With one fold, datarecord applies one set of rules to all three forms. A reader of a hundred-layer tree calls the same members as a reader of one directory. A tool builds a model without handling layers.

Even a plain directory is read through the fold, as a tree of one layer. That fold scans each attribute's rows once to build the owner map. A directory needs the scan in any case. [`flags`](../reference/record.md) reports which dims each attribute's rows set or leave NULL. No parquet footer states that for a file that mixes a series and a constant.

The protocol is structural, so a framework's own object may satisfy it too. A PyPSA network presented as a record depends on nothing in datarecord. The code that reads it cannot tell it from a `Record` ([present a framework object as a record](../howto/present-a-framework-object.md)).

## Frameworks and workflows

A record sits between two kinds of software that need not know each other. A workflow produces a record and branches its scenarios. A framework such as PyPSA reads the record and builds a model. Neither reads the other's code, and core datarecord depends on neither.

A tool translates between a record and one framework. It is a module under `datarecord.tools` that you import explicitly. Importing datarecord pulls in no framework, and importing one tool pulls in no other ([tools](../reference/tools.md)).

The rejected alternative is a schema that contains a framework's vocabulary, such as which attributes a component type takes and which attributes a solve fills. The writer could then enforce those lists. datarecord leaves both to the tool, because the framework's own registry already lists them. A copy in the schema would make datarecord enforce a vocabulary it never defines.

The cost is a looser record. `set("p_max_pu", 0.9)` writes one row that applies to every entity, loads included. The PyPSA tool leaves out and reports a value that a component type does not take. With the vocabulary in the schema, datarecord would refuse the write instead.

## One engine

A record exposes DuckDB relations, and DuckDB is the one engine from the fold to the reader. A relation is a query plan, so listing every attribute reads no data, and you decide when to fetch with `.df()`, `.pl()` or `.to_arrow_table()`.

The rejected alternative is an abstraction over engines: a dataframe wrapper such as narwhals around each relation, or an engine interface inside the fold. Every record is DuckDB underneath, so a wrapper adds a conversion to each read and write, and gives a caller nothing it uses. The fold is relational algebra of real complexity. An engine interface with one implementation would be work done for a second engine that does not exist.

The cost is DuckDB as a hard dependency. A reader written in polars or pandas expressions uses SQL or relation calls instead, or fetches a frame first. No `RecordLike` can be backed by another engine, and a tool plans a framework object's relations on the DuckDB connection the caller passes it.

## Rows as written

A record exposes an attribute's rows as its layers wrote them, with each NULL left NULL. A NULL in a dim's column means every label of its dim, which is [the broadcast rule](../reference/data-model.md). You, or a tool, apply the rule with `settle` when you need one value per point ([read a record](../howto/read-a-record.md)).

The rejected alternative is a read that applies the rule first. It would expand a constant `p_max_pu` to one row per snapshot, and DuckDB would run the expansion for every reader, needed or not. With rows as written, a value stored once is returned once. A framework that stores a constant apart from a series then tells the two apart without comparing values.

The cost is a step that the reader must run. A reader that ignores the rule takes a NULL in a dim's column for a missing label. Where two rows cover one coordinate, datarecord guarantees that one sets every dim the other sets. So `settle` always keeps one row at each coordinate ([resolution](../reference/resolution.md)).

## What is out of scope

datarecord covers the `RecordLike` protocol and `WorkingRecord`, the data model and its schema, layers and the fold, the write path, and the parquet format. An implementation on an engine other than DuckDB is out of scope, for the reasons above. So is a layer stored as anything but parquet, since a layer of a store and a standalone directory share one layout ([the format](../reference/format.md)).

What a framework does with a record is out of scope too. PyPSA's documentation covers how PyPSA builds or solves a network. The PyPSA tool's reference states only what the tool reads from a record and writes back.

## What you can now judge

You can now judge whether your framework can read a record through `RecordLike` alone, and whether DuckDB as a hard dependency suits your code.
