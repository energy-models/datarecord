<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Reference

These pages describe what datarecord does: what a record contains, what each call accepts, returns and refuses, and what is on disk.

## A record and its data

What a record contains, and the calls that read, select and edit it.

- [The data model](data-model.md): dimensions, multi-dimensions, properties, categories, attributes and the broadcast rule.
- [The schema](schema.md): every key a schema accepts, its dtypes, names and compatible changes.
- [The record](record.md): the `RecordLike` protocol, relations, `flags` and `settle`.
- [Selecting](selecting.md): `select`, squeezing, dropped labels and `lift`.
- [Editing](editing.md): `WorkingRecord`, `set`, `add`, `remove` and `edits`.

## Layers and storage

How revisions stack, how a read combines them, and what is on disk.

- [Layers](layers.md): revisions, the tree, `partial` and removing labels.
- [Resolution](resolution.md): the fold, the owner map and crossing rows.
- [The record format](format.md): the files of a record on disk.
- [Workspace and store](store.md): `Workspace`, `Store` and their files.

## Tools and lookup

The tools that hand a record to a framework, and where to look up a word or a signature.

- [Tools](tools.md): the tool's schema and the PyPSA tool.
- [Glossary](glossary.md): each project word and the page that defines it.
- [Python API](api/index.md): every public name, rendered from its docstring.
