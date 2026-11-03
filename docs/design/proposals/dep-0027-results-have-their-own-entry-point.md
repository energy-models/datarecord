<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 27: results have their own entry point

| DEP      | 27                                                                                                 |
| -------- | -------------------------------------------------------------------------------------------------- |
| Title    | results have their own entry point                                                                 |
| Author   | Bryn Pickering                                                                                     |
| Status   | Draft (2026-10-29)                                                                                 |
| Requires | [DEP 17](dep-0017-results-are-attributes.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) |
| Created  | 2026-10-29                                                                                         |
| Version  | Unreleased                                                                                         |
| Replaces | [DEP 17](dep-0017-results-are-attributes.md) (its `results_only` and `base` on `to_datarecord`)    |

## Abstract

A tool writes a solve's results with `results(model, base)`, a method of its own. `to_datarecord(model, con)` is the full export alone, and loses `results_only` and `base`. `results` takes its connection from `base`, so a results write names the solved model and the record it was built from, and nothing else.

## Motivation

**Two arguments that always travel together.** [DEP 17](dep-0017-results-are-attributes.md#the-tool-interface) folded `Tool.results` into `to_datarecord(model, con, *, results_only=False, base=None)`. `results_only=True` requires `base`, and `base` is refused without it, so of the four combinations two raise a `ValueError` at run time. Every results write spells both:

```python
solved = store.commit(
    PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record),
    parent=inputs,
)
```

**A third argument the second already carries.** `con` must be the connection `base`'s relations are planned on, or the thin layer cannot join against `base`. The PyPSA tool already reads that connection off a `Record` in `build`; on the results path the caller restates it.

**A workflow that writes results in a loop says it most.** A myopic or rolling-horizon run builds, solves and writes results once per period, and its authors asked for `PyPSA.results(n, revision)`: the one call that writing results is.

## Specification

### The tool interface

| today                                                                            | under this DEP                                 |
| -------------------------------------------------------------------------------- | ---------------------------------------------- |
| `to_datarecord(model, con, *, results_only=False, base=None)`                    | `to_datarecord(model, con)`: the full export   |
| `to_datarecord(model, con, results_only=True, base=record)`                      | `results(model, base)`: the thin results layer |
| `ValueError` for `base` without `results_only`, or `results_only` without `base` | gone: no combination to refuse                 |

```python
class Tool(Protocol):
    def to_datarecord(self, model: Any, con: DuckDBPyConnection) -> RecordLike: ...
    def results(self, model: Any, base: RecordLike) -> RecordLike: ...
```

- **`results` presents what `results_only=True` presents today**, unchanged: the result attributes, at their default too, and the intermediates they need; `base`'s result rows the solve does not write again restated as NULL; the `sub_network` axis restated with this solve's SubNetworks ([writing results](../tools/pypsa.md#writing-results)).
- **Its connection is `base`'s.** The PyPSA tool reads it as `build` does, and refuses a `base` that is not a `Record` with the `TypeError` `build` raises.
- **It vouches for unique coordinates by the solve alone.** Where [PR #116](https://github.com/energy-models/datarecord/pull/116) has `_ResultsSource.is_unique` also ask `base`, that half holds for every `Record`, so it holds for every `base` `results` takes.
- **`to_datarecord` is the full export over nothing**, as `results_only=False` is today.
- **Both refuse what they refuse today**: names two entity types share, and a clashing static column, as `UnsupportedRecordError`.
- **A collection passes through either.** Where `to_datarecord` takes a `NetworkCollection` with `collection_dim`, as an in-flight branch has it, `results(model, base, *, collection_dim=None)` takes it the same way, and the rule tying `collection_dim` to a collection moves with it.

## Rationale

**Two scopes, two methods.** [DEP 17](dep-0017-results-are-attributes.md#rationale) kept one `to_datarecord` so the rule for each result lives in one place. That holds without the flag: the PyPSA tool already presents the two through `_NetworkSource` and `_ResultsSource`, and `to_datarecord` only picks one. A method per scope makes the invalid combinations unwritable rather than refused, and names the call by what it writes.

**`base` is the subject.** A results layer means nothing without the record it goes over, so `base` is positional and required, and the connection follows from it.

**The name.** `results` is the name the downstream ask uses, and the one DEP 17 retired for a method returning frames. Pre-1.0, it is free to mean the layer.

**What breaks:**

- Every `to_datarecord(..., results_only=True, base=...)` call becomes `results(model, base)`.
- `to_datarecord(..., results_only=False)` or `base=None` passed explicitly is refused as an unknown keyword.
- A `Tool` implementation outside the package adds `results`; one that only exports implements it by raising.
- A results write over a `RecordLike` that is not a `Record` is refused, where today any `RecordLike` whose relations are planned on `con` serves.

## Usage

The examples write with `store.commit`, which replaces `store.root` and `rev.child` in [PR #119](https://github.com/energy-models/datarecord/pull/119). `results` does not depend on it: over today's API, `store.commit(source, parent=rev)` is `rev.child(source)`, and `store.commit(w)` is `rev.child(w.edits)` over the revision `w` was built on.

Before:

```python
inputs = store.commit(w)
n = PyPSA.build(inputs.record)
n.optimize()
solved = store.commit(
    PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record),
    parent=inputs,
)
```

After:

```python
inputs = store.commit(w)
n = PyPSA.build(inputs.record)
n.optimize()
solved = store.commit(PyPSA.results(n, inputs.record), parent=inputs)
```

A myopic run, one solve per period, each over the last:

```python
rev = first  # the revision holding the first period's inputs
for period in periods:
    w = WorkingRecord(rev.record)
    retire_and_extend(w, period)
    rev = store.commit(w)
    n = PyPSA.build(rev.record)
    n.optimize()
    rev = store.commit(PyPSA.results(n, rev.record), parent=rev)
```

A full export is unchanged but for the dropped keywords:

```python
rev = ws.store(uri).commit(PyPSA.to_datarecord(n, ws.con), parent=None)
```

## Implementation notes

1. `tools/base.py`: `Tool.to_datarecord` loses `results_only` and `base`; `Tool.results(model, base)` added.
2. `tools/pypsa.py`: `results` returns `_ResultsSource(model, _connection(base), base)`; `to_datarecord` returns `_NetworkSource`; the flag check goes.
3. Tests: `results_only` calls in `test_tools.py` move to `results`; the test of the refused combinations goes, and one for a non-`Record` `base` is added.
4. Pages: [tools](../tools/index.md#presenting-a-framework-object), [PyPSA](../tools/pypsa.md#writing-results), the [tools usage page](../../usage/tools.md), `CHANGELOG.md`.

Checks that hold once built:

- **No `results_only` in `src/`, `tests/` or the design and usage pages.**
- **A solved network round-trips** through `results` as through `results_only=True` today, re-solve included.

## Rejected Ideas

**Keeping the flags.** Four combinations for two behaviours, and a caller restating a connection `base` already holds.

**`base` alone selecting the results layer**, `to_datarecord(n, con, base=record)`. One argument fewer, but an optional keyword silently changes what is written, from the whole network to its results, and the call does not say which.

**Taking a `Revision` and returning its child**, `PyPSA.results(n, rev) -> Revision`, as the downstream ask spells it. One call fewer per solve, but a tool would write, where it only presents and the caller writes; and `store.commit(PyPSA.results(n, rev.record), parent=rev)` is one line. A tool that writes would also follow every change to how a store writes a child, as `store.commit` replacing `rev.child` is, where a tool that presents does not.

**An optional `con` for a `base` that is not a `Record`.** The protocol names no connection, so `results` over any other `RecordLike` would need one passed, bringing back the argument this DEP removes. It is refused, as `build` refuses it.

## Open Issues

- **`con` on `to_datarecord`.** A full export has no `base` to take it from, so it keeps `con`. Whether the tool should take a `Workspace` instead is outside this DEP.
- **Results over a subset of `base`.** An in-flight branch has a results write touch only the network's own periods, snapshots and active entities. That changes what `results` presents, not its signature, and is independent of this DEP.
