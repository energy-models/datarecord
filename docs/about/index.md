<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# About

These pages argue for the design of datarecord: what it chose, what it rejected and what each choice costs. The rules themselves are in the [reference](../reference/index.md).

- [One record](one-record.md): why one interface reads a directory and a tree of layers, and what datarecord leaves to others.
- [The data model](data-model.md): why a dim is flat, a multi-dim is one struct column, a category is a dim and a result is an attribute.
- [Layers](layers.md): why a layer is written once, restates an axis whole and overrides one value per label only along `partial`.
- [Storage](storage.md): why parquet and DuckDB, why writing a manifest creates a revision, and why writers never share a file.
- [Open questions](open-questions.md): what the design leaves unsettled, and the questions it has settled.
