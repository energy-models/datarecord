<!--
SPDX-FileCopyrightText: datarecord contributors
SPDX-License-Identifier: CC-BY-4.0
-->

# Prose rules for datarecord

This file belongs to datarecord, and `copier update` leaves it alone.
[SKILL.md](SKILL.md) contains the shared rules, and where the two differ, follow
this file.

These rules also apply to every design enhancement proposal (DEP). A DEP
follows [the docs-writing skill](../docs-writing/SKILL.md) and its
[project.md](../docs-writing/project.md), which sets the DEP process.

## The reader of a page

The reader of a page is a modeller who uses datarecord from Python and has
read no other page in this repository. They declare a schema, fill a record,
branch a scenario, read it back and hand it to a framework such as PyPSA. They
came to find out what a call returns, what a record contains and what happens
if they write X.

The second reader of a page is a tool author, who presents a framework's own
object as a record or writes a tool like the PyPSA one, and who knows that
framework better than datarecord. The reader of a DEP or of a page under
`docs/development/` is a contributor who changes datarecord.

## Flat verbs

SKILL.md sets the rule. datarecord adds these subjects, verbs and actors.

- A layer, a manifest, a schema, a row and a table are inanimate too, and no
  inanimate subject decides, knows or blocks either. "The manifest names its
  parent" and "a property that still holds the label blocks its removal" are
  the faults the existing pages make most often.
- The real actors are datarecord, a function or method such as `select` or
  `Store.commit`, a tool, a reader, a store, DuckDB, a framework, the solver,
  or you. The acting verbs include folds and drops.
- The fold, the writer and the read are named steps of datarecord and act as
  it does: "the fold drops the parent's row".
- A design decision is datarecord's: "datarecord refuses a property on a dim
  with `one_of`", never "the schema refuses".
- A record is the object you hold, so it has, contains and exposes. "A record
  exposes its attributes as DuckDB relations" is right, and "the record reads
  its layers" or "the record decides" is wrong.
- A row sets a dim or leaves it NULL, and never names one. Where rows cover
  one coordinate, datarecord keeps a row, and a read returns a layer's row. No row
  or layer wins.
- No layer owns a key: "the owner map assigns the key to the layer".
- An attribute is indexed over the dims its values are over: its own `dims`
  plus the dims its multi-dims' fields draw on. A row or a NULL in a dim's
  column applies to or covers labels. Nothing reaches.

## Records and layers

A record is the object, and a layer is data on disk. Write "record" for what
you hold and what `RecordLike` exposes, "layer" for the data one revision
adds, and "directory" for a standalone record on disk. Never write "dataset"
or "model" for a record. "Network" is PyPSA's object, and "model" is what a
framework builds and a solver takes.

## Spelling and punctuation

British spelling applies in DEPs too. The prose has "categorises" and the
method stays `Schema.categorizing`. No em dashes either: write two sentences,
a colon or a list.

## Glosses and history

The [glossary](../../../docs/reference/glossary.md) lists which page defines
each project word, so link the term to that page at first use.

History such as "replaces groups" belongs in git, the changelog and the DEPs.

## Examples in datarecord's words

These restate examples from SKILL.md in this project's terms. Use them when a
generic example does not settle a sentence.

- Define by category first: "A multi-dim is a dim whose labels are tuples of
  other dims' labels." A reference entry is a definition, then behaviour, then
  its keys or arguments.
- Gloss: "the data one revision adds (a layer)", not "a layer (the data one
  revision adds)".
- Gate item 3: "a selection writes nothing. A `WorkingRecord` does that" has a
  pronoun that points at nothing.
- Gate item 4: "The value of that column, which a reader that settles rows
  expands over the tuples, is a whole tuple or NULL" becomes "The column's
  value is a whole tuple or NULL." "The application of the broadcast rule",
  "the creation of a claim" and "the evaluation of a selection" become "when
  `settle` applies the broadcast rule", "when a store claims an id" and "when
  `select` runs".
- Gate item 5: "A NULL in a dim's column is not a missing value, not a zero
  and not a label of the dim" becomes "A NULL in a dim's column means every
  label of its dim."
- Gate item 6: "With edits pending, …" and "Once the store commits the layer,
  …" name the situation, and "on disk" does not.
- Gate item 10: "a record is what it answers, not how it is stored" is an
  aphorism.
- Gate item 11: "restates the axis whole, with every label it keeps" is
  "restates" with its definition hung on the end.
- Gate item 12: "When you leave out `entity`, `set` writes one row that
  applies to every entity" ends on the point.
- Gate item 13: "the writer refuses the layer if a row has a removed label",
  not "is refused when written".

## Project words

Each word in the table has the meaning given and no other. A key or a class
keeps its code spelling in backticks: `dimensions` is the key and `Dimension`
the class, and in prose the word is _dim_.

| Project word       | Define it at first use as                                                                                                                                                                            | Not                                                       |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| a record           | the object you hold: dims, properties and attributes under one schema, exposed as DuckDB relations through `RecordLike`. `Record`, `WorkingRecord` and `Selection` are records                       | dataset, network, model                                   |
| a dim              | a set of unique labels that values vary along, declared by a `Dimension` under `dimensions`                                                                                                          | dimension (except as the gloss), index, coordinate, field |
| an axis            | the labels one dim has in one record, in order, which `record.dims[dim]` returns                                                                                                                     | member order (write axis order)                           |
| a label            | one value of a dim, unique within it. A multi-dim's labels are tuples                                                                                                                                | member, element, entry, name                              |
| a flat dim         | a dim declared without `over`, whose labels are single values. `Schema.flat_dims` lists them, and `Schema.dims` lists them before the multi-dims                                                     | plain dim                                                 |
| a multi-dim        | a dim declared with `over`, whose labels are tuples of other dims' labels and list the combinations that exist, such as `connection` over `(entity, bus)`                                            | group, mapping, relation                                  |
| a field            | one named part of a dim's label, such as `bus` of `connection`; a flat dim has one, named for itself. `set` and `select` take labels by field                                                        | coordinate, dim, column (for a multi-dim's field)         |
| a column           | where a dim is stored in a file or relation, one per dim: a flat dim's labels, or a multi-dim's fields packed in one struct column                                                                   | field, dim column                                         |
| a coordinate       | one label on each dim of an attribute, such as `(gas, high)`, never NULL, which is where a value applies. A row's key covers coordinates, and a NULL in a dim's column covers every label of its dim | point, cell, location, field                              |
| a property         | a value per label of its subject dim, declared by a `PropertySpec` under `properties` and stored as a column of that dim's axis file. You name it `subject.name`                                     | trait, member attribute, metadata                         |
| a category         | a label's value of a categorising property, which is a property of a dim in `partial` that refers to a dim with `one_of`, such as `entity.ctype`                                                     | type, class, kind, trait                                  |
| an attribute       | the values at coordinates over the dims its `AttributeSpec` lists, stored as long rows, one file per attribute. A solve's results are attributes too                                                 | parameter, variable, field, result (as a kind of its own) |
| the broadcast rule | the rule that a NULL in a dim's column means every label of its dim, and that where rows cover one coordinate, datarecord keeps the row that leaves no dim NULL that another row there sets          | wildcard, default row, fill                               |
| a schema           | the `Schema` of a record: the dims, properties, attributes and `partial` that may exist, with their dtypes. A tool's attribute map is "the tool's schema"                                            | spec, model                                               |
| a layer            | the data one revision adds on top of its parent: a directory under `layers/<id>/`, written once                                                                                                      | patch, overlay, node, delta                               |
| a revision         | a `Revision`: the id of one layer in a store, whose manifest lists its parent. Revisions form a tree                                                                                                 | node, version, commit, snapshot                           |
| the fold           | the step that builds the owner map along a revision's ancestry, one layer at a time, so that a read returns the deeper layer's row where rows of two layers meet                                     | merge, overlay, resolve (for this step alone)             |
| the owner map      | the fold's result: per attribute, each key a layer wrote and the layer the map assigns it to                                                                                                         | ownership table                                           |
| to cross           | of two rows of two layers, to cover a common coordinate where each row leaves NULL a dim that the other row sets. The fold then adds an overlap row that sets both, assigned to the deeper layer     | conflict, clash, collide                                  |
| indexed over       | of an attribute: each dim its `dims` lists, and each dim that a field of one of its multi-dims draws on                                                                                              | reaches, spans                                            |
| `partial`          | the `Schema.partial` set of dims along which a layer overrides one value per label. Along any other dim a layer restates the whole extent of each key it touches                                     | sparse, patchable, granularity (in prose)                 |
| to materialise     | to write a revision's owner map and pointers to its resolved axes under `resolved/`, with `Revision.materialise()`, so that a read stops at that revision                                            | node cache, cache (for this)                              |
| to remove a label  | to restate its dim's axis without it, which removes its values and the tuples that contain it                                                                                                        | delete, tombstone, dead label (the code's word)           |
| an edit            | a `set`, `add` or `remove` on a `WorkingRecord`, staged until `store.commit` writes the edits as a child layer                                                                                       | change, patch, mutation                                   |
| a manifest         | a `Manifest`: a layer's `manifest.json`, which lists its id, parent, full schema, files and meta. A store writes it last and create-only, and the revision exists from that moment                   | index, metadata file                                      |
| a claim            | a `Claim`: a revision id that a store reserves before the layer is written. `claim.commit` writes the layer                                                                                          | lock, reservation                                         |
| a store            | a `Store`: one location, local or on S3, that contains the layers of a tree of revisions                                                                                                             | repository, database, bucket                              |
| a workspace        | a `Workspace`: the DuckDB database and the file access that its stores share, so records from several stores join                                                                                    | session, connection, environment                          |
| a reader           | code that reads a record through `RecordLike`, such as a tool or your script. The person reading a page is "you"                                                                                     | consumer, client                                          |
| a tool             | a module under `datarecord.tools` that translates between a record and one modelling framework, with `build`, `verify` and `to_datarecord`                                                           | adapter, plugin, exporter, converter                      |
| a framework        | a modelling framework, such as PyPSA, that builds a model from a record                                                                                                                              | engine (DuckDB is the engine), backend                    |
| a selection        | the `Selection` that `select(record, **labels)` returns: a record at the labels named                                                                                                                | view, subset, slice, filter                               |
| to squeeze         | to select one label of a dim, which drops the dim from the selection's schema. A list of one keeps the dim                                                                                           | collapse, drop                                            |
| to lift            | to put the squeezed labels back on rows written over a selection, with `Selection.lift`                                                                                                              | expand, restore                                           |
| a registered view  | a DuckDB view that `Record.register` creates, one per dim, property subject and attribute, for a SQL client                                                                                          | view (alone)                                              |
| to settle rows     | to apply the broadcast rule to long rows with `settle(rel, labels, flags)`: one row per label passed at each coordinate, where datarecord keeps the most specific row                                | resolve, expand, densify                                  |
| flags              | what `record.flags(**categories)` returns: per attribute, the dims some row sets (`varies`) and the dims some row leaves NULL (`broadcast`)                                                          | shape, layout                                             |

## Words to avoid

| Avoid                                                        | Write instead                                   |
| ------------------------------------------------------------ | ----------------------------------------------- |
| entity type                                                  | `ctype`, or the component type in PyPSA's words |
| simply, just                                                 | nothing: cut them                               |
| under the hood, seam, surface, lane, closure and bookkeeping | the plain thing                                 |
| owned whole, whole-owned                                     | outside `partial`                               |
| named set (of dims)                                          | set of dims                                     |
| parquet reader                                               | parquet library, such as pandas or polars       |

## Extra message rules

These override two rules of SKILL.md. A message is one sentence that starts
in lower case and ends without a full stop, which overrides the
two-sentence example under "Say what is wrong". The rewrite follows a
semicolon, which overrides "No semicolons" for messages alone.

- The message names the call or the key, states the fault, and then gives the
  rewrite after a semicolon: "`` `set('p_nom', ...)` `` keys its values by
  'entity' and names it as a keyword too; give its labels one way".
- Quote the call, the key, the dim or the label with `!r` or in backticks, as
  the module already does. A property is `subject.name`.
- The exception's class name opens the line the reader sees, so the message
  starts in lower case.
- Link the page that defines the rule in parentheses at the end, as the full
  URL on the published site:
  `(https://energy-models.github.io/datarecord/reference/schema/#names)`.
  Link a reference page, and never a DEP.
- When a page moves, the redirect in `mkdocs.yml` keeps the old URL working,
  and the message moves to the new URL in the same PR.
- A context prefix is a call or a key, such as
  `` `select(scenario='mid')` `` or `property bus.x`.
- A test that matches the text uses `pytest.raises(match=...)`, and changes in
  the same commit as the message.
