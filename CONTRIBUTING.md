<!--
SPDX-FileCopyrightText: datarecord contributors
SPDX-License-Identifier: CC-BY-4.0
-->

# Contributing

Contribution rules and conventions for datarecord.

## Development workflow

- Manage the environment with [`pixi`](https://pixi.sh) and run every command
  inside it: `pixi run <command>` (e.g. `pixi run pytest`).
- Run the test suite with `pixi run test`, and lint, format and type-check with
  `pixi run lint` before every commit — it runs the full lefthook hook set
  (ruff, prettier, taplo, typos, zizmor, reuse and mypy).
- Lockfiles must stay consistent with package metadata: after any change to
  `pixi.toml`, run `pixi lock`.
- The per-python environments (`py311` … `py314`) mirror CI.

## Project conventions

- Branch off `main` for every change and open pull requests via the GitHub CLI
  (`gh`).
- Write tests for new features and bug fixes under `tests/` as `test_*.py`,
  reusing the shared fixtures in `tests/fixtures.py` and `tests/conftest.py`
  where useful. Run the tests after making changes and make sure they pass.
- [`docs/design/`](docs/design/) is the authoritative design, published at
  <https://energy-models.github.io/datarecord/design/>. Cite its pages from a
  docstring's numpydoc `Notes` section rather than restating the argument — a
  comment that re-argues the design is a defect. When behaviour changes, update
  the page, not just the code. (`Notes`, not `References`: numpydoc discourages
  web links under `References` and expects entries there to augment a docstring
  rather than be required to understand it, which these are.)
- Documentation is mkdocs: `pixi run -e docs docs` serves it locally, and
  `pixi run -e docs docs-build` is the strict build CI runs, which fails on a
  broken cross-reference. Every pull request publishes a rendered preview to
  `https://energy-models.github.io/datarecord/pr-<N>/`, linked from a comment on
  the pull request itself; it is removed when the pull request closes, and a
  weekly job sweeps any that outlive it. Both live in
  [`.github/workflows/docs.yml`](.github/workflows/docs.yml).
- No tool import may leak into core `datarecord`
  ([module layout](https://energy-models.github.io/datarecord/design/module-layout/)):
  everything framework-specific lives under `datarecord/tools/` behind an
  optional extra.

## Architecture in one paragraph

datarecord stores dimensioned attribute data with a declared schema: components
(named members of a type, unique record-wide), connections, attribute values over
both, and the axes those values vary along. A record is defined by the `Record`
protocol — what it answers, not how it is stored — and a parquet directory is its
on-disk form. Two implementations serve that protocol — `DirectoryRecord` over a
single directory, and `LayeredRecord` over a tree of layers resolved
last-writer-wins — so a consumer cannot tell which it holds. Queries are built
with `narwhals` and executed by `duckdb`, staying lazy until collected. Beyond
those and `pydantic`, core depends on nothing. Keep new features consistent with
this schema-declared, backend-agnostic, lazily-evaluated design.

## AI-assisted contributions

If you use AI tools when contributing, please read `AGENTS.md`
for how AI-generated content must be marked and what we expect you to write
by hand.

## Licensing

Copyright (c) 2026 datarecord contributors.
By contributing to datarecord, i.e. through opening a pull request, you represent that your contributions are your own original work and that you have the right to license them, and you agree that your contributions are licensed under the .

## Reporting bugs and requesting features

You can open an issue on GitHub to report bugs or request new datarecord features.
Follow these links to submit your issue:

- [Report bugs or other problems while running datarecord](https://github.com/energy-models/datarecord/issues/new?template=BUG-REPORT.yml).
  If reporting an error, please include a full traceback in your issue.

- [Request features that datarecord does not already include](https://github.com/energy-models/datarecord/issues/new?template=FEATURE-REQUEST.yml).

- [Report missing or inconsistent information in our documentation](https://github.com/energy-models/datarecord/issues/new?template=DOCS.yml).

- [Any other issue](https://github.com/energy-models/datarecord/issues/new).

## Submitting changes

Look at the [development guide in our documentation](https://energy-models.github.io/datarecord/contributing) for information on how to get set up for development.

<!--- the "--8<--" html comments define what part of this file to add to the index page of the documentation -->
<!--- --8<-- [start:docs] -->

To contribute changes:

1. Fork the project on GitHub.
1. Create a feature branch to work on in your fork (`git checkout -b new-fix-or-feature`).
1. Test your changes using `pixi run test`.
1. Commit your changes to the feature branch (you should have `pre-commit` installed to ensure your code is correctly formatted when you commit changes).
1. Push the branch to GitHub (`git push origin new-fix-or-feature`).
1. On GitHub, create a new [pull request](https://github.com/energy-models/datarecord/pull/new/main) from the feature branch.

When you contribute for the first time, ensure your reviewer [adds you as a contributor](https://allcontributors.org/en/bot/)!

### Pull requests

Before submitting a pull request, check whether you have:

- Added your changes to `CHANGELOG.md`.
- Added or updated documentation for your changes.
- Added tests if you implemented new functionality.

When opening a pull request, please provide a clear summary of your changes!

### Commit messages

Please try to write clear commit messages.
One-line messages are fine for small changes, but bigger changes should look like this:

```text
A brief summary of the commit (max 50 characters)

A paragraph or bullet-point list describing what changed and its impact,
covering as many lines as needed.
```

### Code conventions

Start reading our code and you'll get the hang of it.

We mostly follow the official [Style Guide for Python Code (PEP8)](https://www.python.org/dev/peps/pep-0008/).

We have chosen to use the uncompromising code formatter and linter [`ruff`](https://beta.ruff.rs/docs/).
When run from the root directory of this repo, `pyproject.toml` should ensure that formatting and linting fixes are in line with our custom preferences (e.g., maximum line length).
To make this a smooth experience, you should run `pixi run pre-commit install` after setting up your development environment.
If you prefer, you can also set up your IDE to run these two tools whenever you save your files, and to have `ruff` highlight erroneous code directly as you type.
Take a look at their documentation for more information on configuring this.

We require all new contributions to have docstrings for all modules, classes and methods.
When adding docstrings, we request you use the [Google docstring style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).

## Release checklist

### Pre-release

- Make sure all unit and integration tests pass (This is best done by creating a pre-release pull request).
- Make sure documentation builds without errors (`pixi run docs-build`).
- Make sure the [changelog](CHANGELOG.md) is up-to-date, especially that new features and backward incompatible changes are clearly marked.

### Create release

- Bump the version number in `src/datarecord/__init__.py`
- Update the [changelog](CHANGELOG.md) with final version number of the form `vX.Y.Z` + release date.
- Commit with message `Release vX.Y.Z`, then add a `vX.Y.Z` tag.
- Create a release pull request to verify that all CI and CD checks pass.
- Once the PR is approved and merged, create a release through the GitHub web interface, using the same tag, titling it `Release vX.Y.Z` and include all the changelog elements that are *not- flagged as **internal**.

### Post-release

- Update the changelog, adding a new `[Unreleased]` heading.
- Update `src/datarecord/__init__.py` to the next version appended with `.dev0`, in preparation for the next main commit.

<!--- --8<-- [end:docs] -->
