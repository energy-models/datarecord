<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The data model

datarecord stores each construct as flat as it can, so that datarecord and a reader apply one rule to every instance of it.

## Why a dim is flat

A dim is a set of unique labels, stored as one column, and every dim is declared in the schema ([the data model](../reference/data-model.md)). datarecord fixes no list of axes, so a record with `region`, `technology` or `vintage` needs a schema entry and no code change.

The rejected alternative is a nested dim, labelled by its parent's label and its own. A multi-period snapshot would be `(period, snapshot)`, and every series over `snapshot` would contain its period. Each check that placed a label by one column would need a branch for two. mathspec, which supplies datarecord's dtypes, gives every dimension one flat list of labels, so a tool would flatten the label anyway.

datarecord states the nesting in one of two flat forms instead. A property maps each label to a label of another dim: `bus.country` names each bus's country, and you read it as you read `bus.carrier`. A multi-dim names both in each label: PyPSA's multi-period `snapshot` is a multi-dim over `(period, timestep)`, one struct column ([below](#why-a-multi-dim-is-one-struct-column)). With one column per dim either way, datarecord applies one rule to the axis, the owner map's index and the broadcast rule. No dim has a parent, so no series can leave a parent's label NULL.

A source whose labels repeat across parents takes the multi-dim form. PyPSA reuses its timesteps in every period, so the PyPSA tool makes each snapshot the tuple of its period and timestep. "The same timestep in every period" is then the field keyword `timestep=[t]`, which selects that tuple in every period.

## Why a multi-dim is one struct column

A multi-dim is a dim whose labels are tuples of other dims' labels, such as `connection` over `(entity, bus)`. A multi-dim is one struct column named for it, in its axis file and in every attribute row over it.

The rejected alternative flattens the tuple into a column per field, so an attribute over `connection` has the columns `entity` and `bus`. The column `bus` then means two things: the bus of a connection, and the dim `bus`. A NULL there cannot mean every bus, because a connection's bus has no axis of its own to expand against. Declaring `connection` would stop a per-bus `marginal_price` from having one value for every bus.

With one struct column, the two stay apart. A NULL `connection` means every connection, a NULL `bus` means every bus, and the column's name states which multi-dim a row belongs to. A struct is a whole tuple or NULL, so a row cannot set only part of a tuple.

The other alternatives lose more. Without multi-dims, with valid pairs stated by a condition, the record loses the whole-or-NULL broadcast and the axis file that contains `connection.role`. A surrogate dim with one label per tuple makes you write a value against a port label and look up its entity and bus. Then the port label, not the tuple, is the value's label.

datarecord applies every rule for a dim to a multi-dim. It is declared under `dimensions`, has its file under `dimensions/`, takes `add` and `remove`, and may sit in `partial`. Each of those rules is stated once for both.

The cost is a struct at the boundary. A filter on one field is `connection.entity = 'wind'`, which DuckDB pushes into the parquet scan as it does a plain column. A numpy-backed pandas frame has no struct dtype, so a pandas reader unnests on the DuckDB side first.

## Why a category is a dim with `one_of`

A category is a label's value of a categorising property, such as `entity.ctype`, which refers to the dim `ctype` declared with `one_of: [Generator, Link, Load]`. That dim's axis is the schema's list, and no layer writes or changes it ([the schema](../reference/schema.md)).

datarecord checks a category against the schema's list wherever a `default_by`, `flags` or a selection names it. That check needs the vocabulary in the schema. If a layer could shrink the axis, a category the schema accepted could match no label in the record. The check could then run only against the folded record.

The rejected alternatives each declare the same closed set a second way. An `Enum` dtype beside a reference to a dim would make a closed set categorise or not by how you declared it. A `one_of` list that only bounds an axis a layer writes would make every PyPSA export restate the schema in a file.

A property categorises its subject only where the subject is in `partial`, the set of dims along which a layer overrides one value per label. A default per category is a default per label, so the owner map must assign the subject's map rows label by label.

The cost is that a new category is a schema change. A custom PyPSA component type needs a new schema, not a data edit. [Open questions](open-questions.md) lists whether such a dim may grow in a layer.

## Why properties and attributes differ

A property is one value per label of its subject, stored as a column of the subject's axis file. An attribute is the values at coordinates over the dims it lists, stored as long rows, one file per attribute ([the data model](../reference/data-model.md)). You declare which a name is.

The rejected alternative places a value by its shape: a value over one dim goes on that dim's axis, and anything wider in a file. That groups `p_nom` with `carrier`, which both vary over `entity` alone. Yet `p_nom` is data a model optimises against and a scenario changes, while `carrier` describes the generator. Placing by shape also makes a compatible change move a file. Widening `p_nom` over `scenario` would move it off the axis, away from the rows earlier layers wrote.

When the schema section sets the file, the file never moves. Widening `p_nom` over `scenario` adds a column to `attributes/p_nom.parquet`, and earlier rows are NULL there, which means every scenario.

A property's name belongs to its subject, so `bus.x`, a map position, sits beside the attribute `x`, a line's reactance. The record-wide name rule exists so that one attribute is one file with one dtype, and a property's file is already its subject's.

The cost is a declaration that can be wrong. datarecord does not derive whether a bus's `x` is metadata or data, and moving a name between sections is an incompatible schema change.

## Why results are attributes

A solve's results are attributes like any other, with the same spec, file layout and fold. A child layer inherits its parent's results as it inherits its inputs.

The rejected alternative keeps results apart, in a second mapping, a separate directory or behind a flag on the spec. A result already has an input's spec and file shape. A split would add a branch to every reader in return for one rule about inheritance. Which attributes a solve fills is the framework's vocabulary, which the PyPSA tool reads from PyPSA's registry ([tools](../reference/tools.md)).

datarecord runs every check on results that it runs on inputs, so a reader that joins a result to `dimensions["entity"]` never finds an orphan.

The cost is staleness. A child that edits an input keeps results computed from its parent's inputs, which describe a record that no longer exists. datarecord keeps both and leaves their coherence to you ([open questions](open-questions.md)).

## What you can now judge

You can now judge whether a construct of your framework is a dim, a multi-dim, a property or an attribute, and what each choice costs. [Declare a schema](../howto/declare-a-schema.md) shows each declaration.
