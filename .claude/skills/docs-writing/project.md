<!--
SPDX-FileCopyrightText: datarecord contributors
SPDX-License-Identifier: CC-BY-4.0
-->

# Docs rules for datarecord

This file belongs to datarecord, and `copier update` leaves it alone.
[SKILL.md](SKILL.md) contains the shared rules, and where the two differ, follow
this file.

## Nav sections

Each section does one job for the modeller who uses datarecord from Python.

- **Tutorials** take the modeller from nothing to a record they can read back.
  One builds a record from a schema, commits it to a store and reads it back.
  One branches a scenario, and one solves a record with the PyPSA tool.
- **How-to guides** each finish one task: declare a schema, read a record,
  select labels, edit, branch and materialise, store a record, use the PyPSA
  tool, and present a framework's object as a record. The last is the tool
  author's page.
- **Reference** describes what datarecord does: the data model, the schema's
  keys, the `RecordLike` protocol, `select`, `WorkingRecord`, layers, the
  fold, the format on disk, `Workspace` and `Store`, and the tools. The
  glossary lists each project word with a link to the page that defines it.
- **About** argues for the design: one interface over a directory and a tree,
  the data model's choices, write-once layers and `partial`, and the storage.
  Its open questions page lists what the design leaves unsettled.

The Development section, last in the nav, contains the contributing guide,
which is a snippet of `CONTRIBUTING.md`, the page on where the code lives, and
the design enhancement proposals (see
[Design enhancement proposals](#design-enhancement-proposals)).

## Each kind in datarecord

- A reference page mirrors what it describes, which overrides the SKILL.md
  rule of one page per module: the schema page follows the keys in
  declaration order, the record page follows the protocol's members, and the
  format page follows the directory.
- An about page contains no table of keys.
- A tool's page states only what the tool reads from a record and what it
  writes back. How PyPSA builds or solves a network is PyPSA's documentation.
- An open question goes on `docs/about/open-questions.md`, not on the page
  that raises it. Its heading is the question itself, ending in a question
  mark, and a settled question keeps that form with the answer as the first
  sentence under it.

## The running example

Its dimensions are `entity`, `bus`, `carrier`, `snapshot` and `ctype`, with `ctype` a
dimension with `one_of`. Its multi-dimension is `connection` over `(entity, bus)`, and its
attributes are `p_nom` and `p_max_pu`. Where a page needs more, add the fewest
names you can and say so in the PR.

A schema with two dimensions and one attribute is the smallest example that
teaches. Show a schema next to the files it leads to, as a call sits next to
the frame it returns. A quoted message ends with the rewrite and the page that
defines the rule, so a truncated quote drops the half that teaches.

## Examples in datarecord's words

These restate examples from SKILL.md in this project's terms.

- Openings: "A dimension is a set of unique labels that values vary along", "A
  layer's data is written once", "This page describes every key a schema
  accepts".
- How-to headings: "Branch a scenario", "Choose between a property and an
  attribute". Tutorial headings: "The schema", "A child revision". Reference
  headings: "`dimensions`", "`one_of`", "`select`". About headings: "Why a
  layer is written once", "One engine".
- Other headings: "Crossing rows", "Where a row applies". "Labels outside
  the key", not "The fold cannot see a label outside the key". "Concurrent
  writers", not "Writers never share a file".
- A list item that skims: "A NULL in a dimension's column means every label of its
  dimension. A `p_max_pu` row with `snapshot` NULL applies at every snapshot."
- A reference entry for `settle`: "Expands each NULL in a dimension's column over
  the labels passed, and keeps the most specific row at each coordinate." "A
  dimension left out of `labels` stays as written." "`flags` sets how much
  work the query does."
- Structure gate: a section about refused schemas under "Categories" is
  misfiled, and a table of `select`'s keywords inside an about page is
  reference.

## Figures

A section of a reference or about page that states what datarecord does to the
rows of an attribute, or to an axis across layers, has a figure. That covers
the fold of a child layer onto its parent, two rows that cross, an override
along a dimension in `partial`, the broadcast rule, removing a label, a multi-dimension's
tuples, and the squeeze and `lift` of `select`. A figure about structure or
flow, such as the tree of revisions or the path of a read, may use labelled
boxes and arrows.

`docs/static/resolution-fold.svg` is the model. Copy its structure.

- Every attribute or axis is a table, one row per row of its file. This is a
  table view, not a grid of dimensions. The note where no row exists reads like "no
  row: `coal` is removed".
- A second dimension is a second column, such as `scenario` beside `entity` in a
  crossing.
- A table's header band has the tint of its kind. In a result table where
  the owner map is the point, each body row has the tint of the layer the
  owner map assigns it to, as in the model.
- Each coloured table has a text label for its kind, such as "parent layer",
  "child layer" or "owner map".
- An arrow's label names the step or the call, then what it does.
- The words in a figure include a layer, the fold, an overlap row and removed.
- The docs environment contains `librsvg`, which overrides the SKILL.md rule
  to fetch `rsvg-convert` with `pixi exec`:

  ```bash
  pixi run -e docs rsvg-convert -w 1520 -b white x.svg -o light.png
  sed 's/currentColor/#ddd/g' x.svg | pixi run -e docs rsvg-convert -w 1520 -b '#1e2129' -o dark.png
  ```

A page that reuses an existing figure from `docs/static/` checks its marks
against the table below first.

## Figure colours

| Mark                                           | Means                                                                                         |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------- |
| blue `#0072B2` tint                            | a parent layer, or data given to the step: the record `select` reads, the rows `settle` takes |
| orange `#E69F00` tint                          | the child layer, or an edit that a `WorkingRecord` stages                                     |
| bluish green `#009E73` tint                    | a result datarecord computes: the owner map after the fold, a read, a selection, settled rows |
| the assigned layer's tint at `0.35`, a bracket | an overlap row, which the fold adds where two rows cross and assigns to the deeper layer      |
| vermilion `#D55E00` hatch                      | a row the fold or a selection drops, or a removed label                                       |
| dashed outline, no fill                        | no row or no file, where one would be                                                         |
| reddish purple `#CC79A7`, sky blue `#56B4E9`   | two sibling layers on separate branches of one parent                                         |
| yellow `#F0E442`                               | not used                                                                                      |

## Generated content

`docs/reference/api/` contains one `:::` entry per public name: the names in
`datarecord.__all__` and the public names of `datarecord.tools`. mkdocstrings
renders them from numpydoc docstrings. This overrides the SKILL.md statement
that `docs/reference/api.md` holds one entry for the package, rendered from
Google-style docstrings.

`docs/development/contributing.md` takes `CONTRIBUTING.md` as a snippet,
`docs/changelog.md` takes `CHANGELOG.md`, and pages take parts of `README.md`.
Edit the source, not the page.

## Extra mechanics

- The build is ProperDocs with Material for MkDocs, from `mkdocs.yml`, and it
  is strict. A dead cross-link, a missing anchor and a page outside the nav
  each fail `pixi run -e docs docs-build`. This overrides the SKILL.md rules
  that name Zensical and `tests/test_docs.py`.
- A page that moves or goes gets a redirect. Add `old.md: new.md` under
  `plugins: redirects: redirect_maps` in `mkdocs.yml`, so that an old URL
  still resolves. Docstrings and messages in a release link the old URL.
- A heading that something links by anchor moves with its links. The strict
  build checks links between pages, but not the full URLs in docstrings and
  messages. Search `src/` for the old anchor before you rename a heading.
- A snippet such as `--8<-- "CONTRIBUTING.md:docs"` takes the part between
  `<!-- --8<-- [start:docs] -->` and `<!-- --8<-- [end:docs] -->`.
- The version lives in `pyproject.toml`, and the changelog and a DEP's
  `Version` field state releases. This overrides the SKILL.md statement that
  the version comes from the git tag.
- Text pulled in from `CONTRIBUTING.md` is rendered under `docs/` too, so its
  links to files outside `docs/` are full URLs.
- Nothing outside `docs/development/proposals/` links to a DEP (see
  [Nothing links to a DEP](#nothing-links-to-a-dep)).

## Design enhancement proposals

A design enhancement proposal (DEP) is a page that argues for one change to
the design of datarecord: a key of the schema, the format on disk, the
`RecordLike` protocol, the fold or the API. Every such change starts as a
DEP, so that the argument is written down before the code. Its reader is a
contributor who was not in the discussion.

### Where a DEP lives

A DEP is `docs/development/proposals/dep-NNNN-slug.md`. Copy the template,
`dep-NNNN-template.md` in the same folder, and take the next free number: one
above the highest number on `main` or in an open PR. A number is never reused
or reassigned, even when its DEP is rejected or its PR closes, so the list may
have gaps. A new DEP adds a nav entry at the end of the proposals section in
`mkdocs.yml`, and a row to the table in `docs/development/proposals/index.md`:
number, title, status and superseded by.

### The header

The template opens with a header table. Its fields are these:

- `DEP` is the number, without leading zeros, and `Title` is the decision as a
  short statement in sentence case. The page's `#` heading is `DEP N: Title`.
- `Author` is who wrote it, and `Created` is the date of the first draft.
- `Status` is one of the statuses below, with the date it took effect:
  `Implemented (2026-10-20)`.
- `Version` is the release the DEP ships in. It is `Unreleased` from
  acceptance until the release PR sets it, and a draft omits it.
- `Requires`, `Replaces` and `Superseded-By` link other DEPs, and a DEP omits
  each one that is empty. `Superseded-By` may name in parentheses the part the
  later DEP replaces.

| Status      | Means                                                                          |
| ----------- | ------------------------------------------------------------------------------ |
| Draft       | the design is under discussion, and the DEP may change in any PR               |
| Accepted    | the maintainers agreed to the design, and the code does not have it yet        |
| On hold     | the discussion is paused, and the DEP may resume or be rejected                |
| Implemented | the code has the design, and the pages it landed in are named under the header |
| Superseded  | a later DEP replaces it, named in `Superseded-By`                              |
| Rejected    | the maintainers declined it, and the DEP stays as the record of why            |

A DEP has landed once its status is Implemented, Superseded or Rejected.

### The body

A DEP is under 1,500 words, as `wc -w` counts the file. The template lists its
sections and what each contains. A DEP is the one page where argument sits
beside specification, which overrides the SKILL.md rule against mixing kinds,
so each section keeps its own kind. Specification describes, as reference
does. Rationale and Rejected ideas argue, as an about page does. Usage shows
calls, as a how-to does. The prose rules apply to a DEP as to any page.

### When a DEP lands

When you implement a DEP, move its content out in the same PR, so that each
piece stands without the DEP:

- The design goes to the reference pages.
- The argument a user needs before relying on a rule goes to the about pages.
- The examples go to the how-to guides, and to a tutorial where one teaches
  the construct.
- What the DEP leaves open goes to `docs/about/open-questions.md`.

Then add two sentences under the DEP's header: "The design landed in
[x](…) and [y](…). Where they and this DEP differ, follow those pages." The
DEP stays as the record of the argument.

A landed DEP's argument and decisions do not change. A change to the design is
a new DEP, which names the old one in `Replaces`. A landed DEP's prose may be
edited to these rules, so long as no decision, number or rejected idea
changes.

### Nothing links to a DEP

Nothing outside `docs/development/proposals/` links to a DEP: not a page, a
docstring, a test or a message. The one exception is
`docs/about/open-questions.md`, which may link the DEP an open question comes
from, and the nav in `mkdocs.yml` lists every DEP. A DEP that has not landed
is not the design, and one that has is replaced by the pages it landed in, so
a link to either sends the reader to the wrong place. The `no-proposal-links`
job in `.lefthook.yaml` fails `pixi run lint` on any other text file that
contains `proposals/dep-` followed by four digits.

## Extra gates

These replace the gates in SKILL.md, because datarecord has no `pixi run ci`
task and builds the docs in the `docs` environment.

```bash
pixi run -e docs docs-build   # strict: a dead link, a missing anchor or a page outside the nav fails
pixi run lint                 # lefthook: ruff, mypy, prettier, taplo, typos, zizmor, no-proposal-links, reuse
pixi run test                 # when a docstring or a message a test matches changed
```
