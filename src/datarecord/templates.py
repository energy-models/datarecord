# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A read built once per plan and slot types, and the SQL text each later call fills it with.

DuckDB binds every step of a relation as it is built, so a read built step by
step costs a bind per step. A `Plan` is hashable data whose `build` makes such
a read from relations, its slots. `Templates` builds it once over the
relations of its first call and keeps it as SQL text, and each later call
writes its own relations' text where the first call's was: the statement is
bound once, as a whole.

Only this module calls DuckDB's private renderer, so a change to the renderer
breaks `text` alone.

Notes
-----
- [registered views](https://energy-models.github.io/datarecord/reference/format/#registered-views)
"""

from __future__ import annotations

import re
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass, field, replace
from functools import partial
from typing import TYPE_CHECKING, Any, Protocol

from duckdb import ColumnExpression as col
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb._relation import _render

if TYPE_CHECKING:
    from datarecord.store.store import Workspace

type Typed = tuple[tuple[str, str], ...]
type Slot = DuckDBPyRelation | Mapping[str, DuckDBPyRelation]

CHECKED = False
"""Whether every call builds its plan afresh too and refuses a template whose text differs, as the test suite's `--check-templates` sets it."""


def text(rel: DuckDBPyRelation) -> str:
    """Return the statement `rel` runs, its values written in, as a view is created of."""
    return _render(rel, literals=True, wrap_text=True)[0]


def typed(rel: DuckDBPyRelation) -> Typed:
    """`rel`'s column names and types."""
    return tuple(zip(rel.columns, map(str, rel.types), strict=True))


class Plan(Protocol):
    """Hashable data whose `build` makes a read of the relations it is passed, its slots.

    Every keyword argument of `build`, one collected by `**kwargs` too, is a
    slot, or a mapping whose every value is one; `con` is its only positional
    argument. `build` must hold to a contract, which `CHECKED` tests:

    - its text depends on the plan's fields and on each slot's columns, types
      and whether it is a table or held rows alone;
    - every value that varies between calls with one plan is in a slot,
      never written into the text;
    - it runs nothing, so a fetch or a materialisation is a statement of its
      own, over the read;
    - each slot's text survives whole in the read's, read once or more.

    Notes
    -----
    - [registered views](https://energy-models.github.io/datarecord/reference/format/#registered-views)
    """

    def __hash__(self) -> int: ...

    def build(
        self, con: DuckDBPyConnection, *args: Any, **kwargs: Any
    ) -> DuckDBPyRelation: ...


class Templates:
    """The templates of a workspace's reads, one per plan and its slots' names and types.

    One cache serves every thread: a template is SQL text, bound by no
    cursor. Two threads missing one key at once build equal templates, and
    either is kept.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
    """

    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self._kept: dict[Hashable, _Template] = {}

    def memoize(self, plan: Plan) -> Templated:
        """Return `plan`'s read, built once per set of slot types and filled with each call's slots."""
        return Templated(self, plan)

    def _filled(self, plan: Plan, named: Mapping[str, Slot]) -> str:
        """`plan.build` over the slots, its text that of the template kept for them, built on a miss.

        Raises
        ------
        TypeError
            Where the slots are not `plan.build`'s keyword arguments, raised
            before a template is kept.
        AssertionError
            Where `CHECKED` is set and `plan.build` breaks the contract `Plan`
            states.
        """
        con = self.workspace.con
        parts = _slots(named)
        held = tuple((n, typed(p), _scanned(p)) for n, p in sorted(parts.items()))
        key = (plan, held)
        build = partial(_built, plan, con, named)
        found = self._kept.get(key)
        if found is None:
            found = self._kept[key] = _template(con, build, parts)
        elif CHECKED:
            new, old = _canonical(_template(con, build, parts)), _canonical(found)
            at = next(
                (i for i, (a, b) in enumerate(zip(new, old, strict=False)) if a != b),
                0,
            )
            assert new == old, (
                f"template {plan!r} varies: {new[at - 200 : at + 200]!r} "
                f"where the kept one reads {old[at - 200 : at + 200]!r}"
            )
        return found.filled(parts)


@dataclass(frozen=True)
class Templated:
    """A plan's read, its template kept in `templates`.

    Called with the slots `plan.build` takes as keyword arguments, it
    returns that read, its statement bound once. A later call's slot
    replaces one of the same name given to `bind`, as `functools.partial`
    passes keywords.
    """

    templates: Templates
    plan: Plan
    kwargs: Mapping[str, Slot] = field(default_factory=dict)

    def bind(self, **named: Slot) -> Templated:
        """Return this read with `named` filled in for every later call."""
        return replace(self, kwargs={**self.kwargs, **named})

    def text(self, **named: Slot) -> str:
        """Return the statement of the read over the bound slots and these.

        Raises
        ------
        TypeError
            Where the slots are not `plan.build`'s keyword arguments.
        """
        return self.templates._filled(self.plan, {**self.kwargs, **named})

    def __call__(self, **named: Slot) -> DuckDBPyRelation:
        """Return the read over these slots, on the calling thread's cursor."""
        return self.templates.workspace.con.sql(self.text(**named))


@dataclass(frozen=True)
class _Template:
    """A statement with a slot per relation it reads, laid out as PEP 750's `string.templatelib.Template` is.

    Attributes
    ----------
    strings
        The text around the slots, one string more than there are slots.
    slots
        The slots' names, in the order they appear in the text, a name once
        per place its relation is read.
    """

    strings: tuple[str, ...]
    slots: tuple[str, ...]

    def filled(self, parts: Mapping[str, DuckDBPyRelation]) -> str:
        """Return the statement with each slot's text that of the relation `parts` gives it.

        Raises
        ------
        ValueError
            Where `parts` names other slots than the template has.
        """
        if parts.keys() != set(self.slots):
            msg = (
                f"the template has slots {sorted(set(self.slots))}, not {sorted(parts)}"
            )
            raise ValueError(msg)
        texts = {n: text(p) for n, p in parts.items()}
        slotted = [texts[s] for s in self.slots]
        return "".join(s + t for s, t in zip(self.strings, [*slotted, ""], strict=True))


def _template(
    con: DuckDBPyConnection,
    build: Callable[[Mapping[str, DuckDBPyRelation]], DuckDBPyRelation],
    slots: Mapping[str, DuckDBPyRelation],
) -> _Template:
    """`build` called with each of `slots` read behind a subquery of its own, rendered as a `_Template` its texts are cut out of.

    The subquery, aliased by the slot's name, is a boundary the renderer
    neither merges across nor lifts a common table expression out of, so
    each relation's text survives whole. A slot's text inside another's,
    which was filled from it, goes with that one.

    The renderer writes the common table expression reading a slot `NOT
    MATERIALIZED`. Over a table or held rows read twice, DuckDB's inliner
    takes seconds to plan, so there it is left to DuckDB to inline or
    materialise; over any other statement it stays, so a filter reaches the
    statement's scans.

    Raises
    ------
    RuntimeError
        Where a slot's text appears nowhere in the rendered statement but
        inside another's, or overlaps another's.
    """
    texts = {n: text(p) for n, p in slots.items()}
    bounded = {n: _bounded(n, t) for n, t in texts.items()}
    whole = text(build({n: con.sql(b) for n, b in bounded.items()}))
    for n in (n for n, p in slots.items() if _scanned(p)):
        whole = whole.replace(_INLINED + bounded[n], _CHOSEN + bounded[n])
    found = sorted(
        (at + len(_OPEN), -len(texts[n]), n)
        for n, b in bounded.items()
        for at in _found(whole, b)
    )
    strings, order, at = [], [], 0
    for start, minus, name in found:
        if start - minus <= at:
            continue
        if start < at:
            msg = f"slot {name!r} overlaps another in the template"
            raise RuntimeError(msg)
        strings.append(whole[at:start])
        order.append(name)
        at = start - minus
    strings.append(whole[at:])
    if set(order) != slots.keys():
        msg = "a slot's text appears nowhere in the template but inside another's"
        raise RuntimeError(msg)
    return _Template(tuple(strings), tuple(order))


def _slots(named: Mapping[str, Slot]) -> dict[str, DuckDBPyRelation]:
    """The slots `named` passes, each by its argument's name, a mapping's as `{argument}.{key}`.

    No argument's name holds a dot, so a mapping's keys never collide with
    another argument's slot, whatever they are. `_built` reads them back.
    """
    out: dict[str, DuckDBPyRelation] = {}
    for name, value in named.items():
        if isinstance(value, Mapping):
            out |= {f"{name}.{k}": v for k, v in value.items()}
        else:
            out[name] = value
    return out


def _built(
    plan: Plan,
    con: DuckDBPyConnection,
    named: Mapping[str, Slot],
    parts: Mapping[str, DuckDBPyRelation],
) -> DuckDBPyRelation:
    """`plan.build` called with `named`, each slot `_slots` names given its relation in `parts`."""
    rebuilt = {
        n: {k: parts[f"{n}.{k}"] for k in v} if isinstance(v, Mapping) else parts[n]
        for n, v in named.items()
    }
    return plan.build(con, **rebuilt)


def _scanned(rel: DuckDBPyRelation) -> bool:
    """Whether `rel` reads a table or held rows as they are."""
    return rel.type in ("TABLE_RELATION", "MATERIALIZED_RELATION")


_OPEN = "SELECT * FROM ("
_INLINED = f"AS NOT MATERIALIZED (\n{_OPEN}\n"
_CHOSEN = f"AS (\n{_OPEN}\n"


def _bounded(name: str, statement: str) -> str:
    """`statement` read as a subquery aliased as the slot `name`."""
    return f"{_OPEN}{statement}) AS {col(f'__slot_{name}')}"


def _found(whole: str, needle: str) -> list[int]:
    """Where `needle` starts in `whole`, each match after the end of the last."""
    out, at = [], whole.find(needle)
    while at >= 0:
        out.append(at)
        at = whole.find(needle, at + len(needle))
    return out


_ALIAS = re.compile(r"\b[A-Za-z_]+_[0-9a-f]{16}\b")


def _canonical(found: _Template) -> str:
    """`found`'s strings and slots, each random relation alias numbered in order of appearance."""
    slots = "".join(f"<{s}>" for s in found.slots)
    whole = "\0".join([*found.strings, slots])
    seen: dict[str, str] = {}
    return _ALIAS.sub(lambda m: seen.setdefault(m[0], f"_alias{len(seen)}"), whole)
