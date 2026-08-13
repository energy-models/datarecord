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
- [`docs/design-documents/data-records.md`](docs/design-documents/data-records.md)
  is the authoritative design. Cite its `§N` sections from docstrings rather
  than restating the argument — a comment that re-argues the design is a defect.
  When behaviour changes, update the section, not just the code.
- No tool import may leak into core `datarecord` (§13): everything framework-
  specific lives under `datarecord/tools/` behind an optional extra.

## Architecture in one paragraph

datarecord stores dimensioned attribute data with a declared schema: components
(named members of a type, unique store-wide), connections, attribute values over
both, and the axes those values vary along. A store is a parquet directory, and
two implementations serve one `Record` protocol — `DirectoryRecord` over a single
directory, and `LayeredRecord` over a tree of layers resolved last-writer-wins —
so a consumer cannot tell which it holds. Queries are built with `narwhals` and
executed by `duckdb`, staying lazy until collected. Beyond those and `pydantic`,
core depends on nothing. Keep new features consistent with this schema-declared,
backend-agnostic, lazily-evaluated design.

## AI-assisted contributions

If you use AI tools when contributing, please read [`AGENTS.md`](AGENTS.md)
for how AI-generated content must be marked and what we expect you to write
by hand.
