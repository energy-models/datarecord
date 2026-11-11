<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Glossary

This page lists each word that datarecord's pages use in one meaning only, with a link to the page that defines it.

| Word                                                   | Meaning                                                                                                                                                                  |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [record](data-model.md)                                | the object you hold: dims, properties and attributes under one schema, exposed through `RecordLike`                                                                      |
| [dim](data-model.md#dims-and-labels)                   | a set of unique labels that values vary along, declared by a `Dimension` under `dimensions`                                                                              |
| [axis](data-model.md#dims-and-labels)                  | the labels one dim has in one record, in order, which `record.dims[dim]` returns                                                                                         |
| [label](data-model.md#dims-and-labels)                 | one value of a dim, unique within it, which is a tuple on a multi-dim                                                                                                    |
| [multi-dim](data-model.md#multi-dims)                  | a dim declared with `over`, whose labels are tuples of other dims' labels                                                                                                |
| [field](data-model.md#multi-dims)                      | one named part of a dim's label: one per entry of `over` on a multi-dim, such as `bus` of `connection`, and one named for itself on a flat dim                           |
| [property](data-model.md#properties)                   | a value per label of its subject dim, named `subject.name`, stored as a column of that dim's axis file                                                                   |
| [category](data-model.md#categories)                   | a label's value of a property of a dim in `partial` that refers to a dim with `one_of`                                                                                   |
| [attribute](data-model.md#attributes)                  | the values at coordinates over the dims its `AttributeSpec` lists, stored as long rows, one file per attribute                                                           |
| [coordinate](data-model.md#attributes)                 | one label on each dim of an attribute, where a value applies                                                                                                             |
| [indexed over](data-model.md#attributes)               | of an attribute: each dim its `dims` lists, and each dim that a field of one of its multi-dims draws on                                                                  |
| [the broadcast rule](data-model.md#the-broadcast-rule) | a NULL in a dim's column means every label of its dim, and where rows cover one coordinate, datarecord keeps the row that leaves no dim NULL that another row there sets |
| [schema](schema.md)                                    | the `Schema` of a record: the dims, properties, attributes and `partial` that may exist                                                                                  |
| [`partial`](layers.md#partial)                         | the dims along which a layer overrides one value per label                                                                                                               |
| [layer](layers.md#revisions)                           | the data one revision adds on top of its parent, written once                                                                                                            |
| [revision](layers.md#revisions)                        | the id of one layer in a store, whose manifest lists its parent                                                                                                          |
| [to remove a label](layers.md#removing-labels)         | to restate its dim's axis without it, which removes its values and the tuples that contain it                                                                            |
| [the fold](resolution.md)                              | the step that builds the owner map along a revision's ancestry, one layer at a time                                                                                      |
| [the owner map](resolution.md#the-owner-map)           | the fold's result: per attribute, each key a layer wrote and the layer the map assigns it to                                                                             |
| [to cross](resolution.md#crossing-rows)                | of two rows of two layers, to cover a common coordinate where each row leaves NULL a dim that the other row sets                                                         |
| [to materialise](resolution.md#materialised-revisions) | to write a revision's owner map and pointers to its axes under `resolved/`, so that a read stops there                                                                   |
| [manifest](format.md#manifestjson)                     | a layer's `manifest.json`: its id, parent, full schema, files and meta                                                                                                   |
| [registered view](format.md#registered-views)          | a DuckDB view that `Record.register` creates for a SQL client                                                                                                            |
| [store](store.md#store)                                | one location, local or on S3, that contains the layers of a tree of revisions                                                                                            |
| [workspace](store.md#workspace)                        | the DuckDB database and file access that its stores share                                                                                                                |
| [claim](store.md#claim)                                | a revision id that a store reserves before the layer is written                                                                                                          |
| [reader](record.md)                                    | code that reads a record through `RecordLike`, such as a tool or your script                                                                                             |
| [flags](record.md#flags)                               | per attribute, the dims some row sets (`varies`) and the dims some row leaves NULL (`broadcast`)                                                                         |
| [to settle rows](record.md#settle)                     | to apply the broadcast rule to long rows with `settle`, one row per label passed at each coordinate                                                                      |
| [selection](selecting.md)                              | the `Selection` that `select(record, **labels)` returns: a record at the labels named                                                                                    |
| [to squeeze](selecting.md#squeezing)                   | to select one label of a dim, which drops the dim from the selection's schema                                                                                            |
| [to lift](selecting.md#lift)                           | to put the squeezed labels back on rows written over a selection, with `Selection.lift`                                                                                  |
| [edit](editing.md)                                     | a `set`, `add` or `remove` on a `WorkingRecord`, staged until `store.commit` writes it as a child layer                                                                  |
| [tool](tools.md)                                       | a module under `datarecord.tools` that translates between a record and one framework                                                                                     |
| [framework](tools.md)                                  | a modelling framework, such as PyPSA, that builds a model from a record                                                                                                  |
