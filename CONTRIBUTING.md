<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Contributing

<!-- --8<-- [start:docs] -->

Contribution rules and conventions for datarecord. We welcome all contributors.
A good place to start is the issues tagged
["help wanted"](https://github.com/energy-models/datarecord/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
and
["good first issue"](https://github.com/energy-models/datarecord/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).

By opening a pull request you represent that your contribution is your own
original work and that you agree to license it under the project's MIT license.

## Reporting issues

Open a GitHub issue to report a bug or request a feature:

- [Report a bug](https://github.com/energy-models/datarecord/issues/new?template=BUG-REPORT.yml)
  and include a full traceback where there is one.
- [Request a feature](https://github.com/energy-models/datarecord/issues/new?template=FEATURE-REQUEST.yml).
- [Report a documentation problem](https://github.com/energy-models/datarecord/issues/new?template=DOCS.yml).
- [Anything else](https://github.com/energy-models/datarecord/issues/new).

## Development workflow

- Manage the environment with [`pixi`](https://pixi.sh) and run every command
  inside it: `pixi run <command>` (e.g. `pixi run pytest`).
- Run the test suite with `pixi run test`, and lint, format and type-check with
  `pixi run lint` before every commit. It runs the full lefthook hook set
  (ruff, prettier, taplo, typos, zizmor, reuse and mypy).
- Lockfiles must stay consistent with package metadata: after any change to
  `pixi.toml`, run `pixi lock`.
- The per-python environments (`py311` … `py314`) mirror CI.

## Project conventions

- Branch off `main` for every change and open pull requests via the GitHub CLI
  (`gh`). A one-line commit message is fine for a small change. A larger one has
  a summary line of at most 50 characters, a blank line, then a body describing
  what changed and why. Before opening a pull request, check you have updated
  `CHANGELOG.md`, added or updated documentation, and added tests for new
  functionality. Give the pull request a clear summary of the change.
- Write tests for new features and bug fixes under `tests/` as `test_*.py`,
  reusing the shared fixtures in `tests/fixtures.py` and `tests/conftest.py`
  where useful. Run the tests after making changes and make sure they pass.
- The [reference](https://energy-models.github.io/datarecord/reference/) and
  [about](https://energy-models.github.io/datarecord/about/) pages are the
  authoritative design. Cite them from a docstring's numpydoc `Notes` section
  rather than restating the argument. A comment that re-argues the design is a
  defect. When behaviour changes, update
  the page, not just the code. (`Notes`, not `References`: numpydoc discourages
  web links under `References` and expects entries there to augment a docstring
  rather than be required to understand it. These pages are required to
  understand it.)
- Documentation is mkdocs: `pixi run -e docs docs` serves it locally, and
  `pixi run -e docs docs-build` is the strict build CI runs, which fails on a
  broken cross-reference. Every pull request publishes a rendered preview to
  `https://energy-models.github.io/datarecord/pr-<N>/`, linked from a comment on
  the pull request itself. The preview is removed when the pull request closes,
  and a weekly job removes any that remain. Both live in
  `.github/workflows/docs.yml`.
- No tool import may leak into core `datarecord`
  ([the package](https://energy-models.github.io/datarecord/development/package/)):
  everything framework-specific lives under `datarecord/tools/` behind an
  optional extra.

### Figures

A reference section that describes what the fold or a read does to rows has a
figure. The
[docs-writing skill](https://github.com/energy-models/datarecord/blob/main/.claude/skills/docs-writing/project.md#figures)
sets how to draw, embed and check it.

## Design enhancement proposals

A change to the design starts as a design enhancement proposal (DEP): a page
in `docs/development/proposals/` that argues for the change before the code
makes it. Copy the
[template](https://energy-models.github.io/datarecord/development/proposals/dep-NNNN-template/)
to `dep-NNNN-slug.md` under the next free number. The
[docs-writing skill](https://github.com/energy-models/datarecord/blob/main/.claude/skills/docs-writing/project.md#design-enhancement-proposals)
sets the rest: the header and its statuses, the word limit, and what moves to
which page when the DEP lands.

A landed DEP's argument and decisions do not change, and a change to the
design is a new DEP. Its prose may be edited to the prose rules, so long as
no decision, number or rejected idea changes.

Nothing outside `docs/development/proposals/` links to a DEP, except the open
questions page. `pixi run lint` fails on such a link.

## Architecture in one paragraph

datarecord stores dimensioned attribute data with a declared schema. A record
has entities (labels of the `entity` dim, unique record-wide) and multi-dims over
them, such as `connection` over `(entity, bus)`. It also has attribute values
over both, and the axes those values vary along. The `RecordLike` protocol defines a record by
the relations it exposes, whatever the storage, and a parquet directory is its
form on disk. The concrete `Record` folds the layers along a revision's
ancestry, and where two layers set the same row, a read returns the deeper
layer's row. `Workspace.at(uri)` reads a single directory as a record, so a
reader gets the same relations however many layers there are. Queries are
DuckDB relations that run only when you fetch them, and the dtypes come from
`mathspec`.
Beyond `duckdb`, `pydantic` and `mathspec`, core depends on nothing. Keep new
features consistent with this schema-declared, DuckDB-backed,
lazily-evaluated design.

## Releasing

The version lives in `[project].version` in `pyproject.toml`, and there is no
VCS-derived versioning. `CHANGELOG.md` tracks user-facing changes under an
`[Unreleased]` heading between releases.

To cut `vX.Y.Z`:

1. Confirm `pixi run test` and `pixi run -e docs docs-build` pass , ideally on
   a release pull request.
2. Rename the `[Unreleased]` heading in `CHANGELOG.md` to
   `vX.Y.Z` with the release date, set `version` in `pyproject.toml`, and set
   `Version` to `vX.Y.Z` in every DEP header reading `Unreleased`.
3. Merge the release pull request, then tag the merge commit `vX.Y.Z` and create
   the GitHub release from that tag.
4. Open a follow-up adding a fresh `[Unreleased]` heading to `CHANGELOG.md`.

## AI-assisted contributions

If you use AI tools when contributing, please read `AGENTS.md`
for how AI-generated content must be marked and what we expect you to write
by hand.

<!-- --8<-- [end:docs] -->
