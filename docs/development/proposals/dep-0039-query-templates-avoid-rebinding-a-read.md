<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 39: query templates avoid rebinding a read

| DEP      | 39                                                |
| -------- | ------------------------------------------------- |
| Title    | query templates avoid rebinding a read            |
| Author   | Jonas Hörsch                                      |
| Status   | Draft (2026-11-17)                                |
| Requires | [DEP 34](dep-0034-a-record-registers-as-views.md) |
| Created  | 2026-11-17                                        |

## Abstract

datarecord introduces a cache of query templates on the workspace, which avoids rebinding a read step by step each time it is built. A template is the SQL text of a read, built once per plan and set of input types. Each later read writes its own inputs into that text, and DuckDB binds the result once, as one statement.

## Motivation

A record's read of an attribute is a chain of relational calls: a join per branch of the owner map, a projection, a union, an anti-join per dimension outside `partial`. Each call returns a new relation, and DuckDB binds it at once, its whole input again, to know its columns. Every `record.attributes[a]` pays for that chain. `register`, which creates a view per attribute ([DEP 34](dep-0034-a-record-registers-as-views.md)), pays for it once per attribute, although most attributes of a record share one chain and differ only in their rows.

## Specification

### Plans

A plan is a frozen dataclass with a method `build(self, con, *slots)` that returns a `DuckDBPyRelation`. Every argument of `build` after `con`, including those that `**kwargs` collects, is a slot: a relation the read takes as input. `datarecord.templates.Plan` is the protocol.

`build` holds to a contract:

- Its text depends on the plan's fields and on each slot's columns, types and whether the slot is a table or held rows, and on nothing else.
- Every value that varies between calls with one plan is a slot, never written into the text.
- It runs nothing, so a fetch or a materialisation is a statement of its own, over the read.
- Each slot's text survives whole in the read's text, read once or more.

### Templates

`Workspace.templates` is a `Templates`, one per workspace. It keeps one template per plan and per slot, with the slot's name, column names, column types and whether it is a table or held rows.

On a miss, `Templates` calls `build` on the calling thread's cursor, with each slot read as `SELECT * FROM (<text>) AS "__slot_<name>"`. It renders the result with DuckDB's renderer and cuts each slot's text out of it. A slot read only inside another slot's text goes with that slot. Where `build` reads a slot nowhere else, or a slot's text overlaps another's, `Templates` raises `RuntimeError`. Where a slot is a table or held rows, its common table expression drops `NOT MATERIALIZED`, so DuckDB chooses whether to inline it.

`templates.memoize(plan)` returns a `Templated`. Called with the slots, it returns the read as a relation on the calling thread's cursor. Its `text` method returns the statement, which `register` creates a view of. The slots bind to `build`'s signature, so a missing or extra slot raises `TypeError`.

### The reads of a record

| Plan         | Fields                                                                                                                                                                          | Slots                                                                                              |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `ReadPlan`   | long columns, dimensions, `partial` dimensions, multi-dimensions' first fields, columns outside `partial`, the ways map rows store each dimension, whether a layer is contested | the owning layers' rows, the attribute's map rows, the removed labels per column outside `partial` |
| `NoRowsPlan` | long columns and their types                                                                                                                                                    | none                                                                                               |

`ReadPlan.from_schema(schema, attribute, owners)` takes from the schema all that the read uses, so no plan has a schema field.

### The check

The test suite's `--check-templates` option sets `datarecord.templates.CHECKED`. Every call then builds its plan afresh too, and asserts that the text equals the kept template's, with DuckDB's random relation aliases numbered in order.

## Rationale

The read stays written in the relational API, so it composes the helpers that other reads share, such as `most_specific` and `null_safe`. Only the bind moves: datarecord binds the chain once per plan, and after that DuckDB binds one statement per read.

The plan is the key. `build` reads nothing but the plan's fields and its slots, so the key cannot miss an input. A value that varies, such as a revision's removed labels, has to be a slot. A plan that writes it into the text fails `--check-templates` at the first call with another value.

A template is text that no cursor binds, so the workspace keeps it for every thread and every record. Two records whose plans and slot types agree share their templates. When two threads miss one key at once, both build equal templates, and either one is kept.

It costs:

- The contract holds by convention. Only `--check-templates` tests it, and only on the reads the suite makes.
- The renderer is private DuckDB API. Only `datarecord.templates` calls it, so a change there breaks one module.
- Every slot is rendered to text on every call, which a read built step by step does not need.
- The workspace never drops a template, so it keeps one per distinct plan and slot types of every schema it reads.

## Usage

A plan is a frozen dataclass:

```python
from dataclasses import dataclass

from duckdb import ColumnExpression as col


@dataclass(frozen=True)
class Positive:
    column: str

    def build(self, con, rows):
        return rows.filter(col(self.column) > 0)


read = workspace.templates.memoize(Positive("value"))
rel = read(rows)
statement = read.text(rows)
```

The first call builds the template, and a later call with rows of the same column types writes their text into it.

## Implementation notes

1. `datarecord.templates` renders a relation's text, and defines `Plan`, `Templates` and `Templated`.
2. `Workspace` creates `templates` on construction.
3. `ReadPlan` builds an attribute's read, and `NoRowsPlan` the read of an attribute no layer holds a row of. `_refusal` reads the attribute off the row, so one template serves every attribute of a plan.
4. `Record._relation` and `Record.register` call the memoized plan, `register` its `text`.
5. `tests/conftest.py` adds `--check-templates`, and `tests/test_templates.py` covers a template that serves two calls, slots that do not bind, and a slot read nowhere but inside another.

The full suite passes with and without `--check-templates`.

## Rejected ideas

- Keeping each attribute's relation: it binds to one cursor, and no two attributes share their rows, so nothing is reused across attributes.
- Writing the read as SQL text by hand: it loses the relational helpers that other reads compose, and two forms of one read drift apart.
- DuckDB's prepared statements: a parameter stands for a value, not for a relation or a table.
- A cache per thread: a template binds no cursor, so each thread would build the same text again.
- A cache key apart from the function that builds the read: the two can disagree, and nothing raises when they do.
- A class decorator that keeps the cache on the plan class: the cache then outlives every workspace, and a reader cannot see where a template is kept.
- Python's template strings ([PEP 750](https://peps.python.org/pep-0750/)): they need Python 3.14, and datarecord supports 3.12. A template here is rendered from relations, not written by hand. The kept template has the same layout, strings with one slot between each pair.

## Open issues

- A DuckDB release that changes its private renderer breaks `text`.
- A long-lived workspace keeps every template it built.
