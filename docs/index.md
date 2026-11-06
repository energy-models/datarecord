<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# datarecord

--8<-- "README.md:docs-pitch"

<figure class="diagram">
--8<-- "docs/static/index-abstract.svg"
</figure>

A schema shapes the base record, two scenarios hold only the rows they change, and reading `high wind` returns one whole record.

--8<-- "README.md:docs-intro"

## Example

--8<-- "README.md:docs-example"

The [scenario tutorial](tutorials/scenario.md) builds this from an empty store in about ten minutes.

## Install

--8<-- "README.md:docs-install"

[Installation](installation.md) lists other package managers.

## Where to go

<div class="grid cards" markdown>

- :material-school: **[Tutorials](tutorials/index.md)**

  Build a first record, branch a scenario and solve a record with PyPSA, one
  step at a time.

- :material-format-list-checks: **[How-to guides](howto/index.md)**

  Finish one task: declare a schema, read, select, edit, branch, store, or
  hand a record to PyPSA.

- :material-book-open-variant: **[Reference](reference/index.md)**

  What datarecord accepts, returns and refuses, and the format on disk.

- :material-lightbulb-on-outline: **[About](about/index.md)**

  Why the design is the way it is, what it rejected, and what it leaves open.

</div>

To change datarecord itself, start at [Development](development/index.md).
