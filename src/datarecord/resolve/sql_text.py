# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A relation's SQL text, and templates whose slots are filled with it.

DuckDB binds every step of a relation as it is built, so a read built step by
step costs a bind per step. A template is such a read built once over the
relations of its first call, and filling it writes each relation's text where
that call's was: the statement is bound once, as a whole.

Only this module calls DuckDB's private renderer, so a change to the renderer
breaks `text` alone.

Notes
-----
- [registered views](https://energy-models.github.io/datarecord/reference/format/#registered-views)
"""

from __future__ import annotations

import re
from collections.abc import Callable, Hashable, MutableMapping
from dataclasses import dataclass

from duckdb import ColumnExpression as col
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb._relation import _render

type Typed = tuple[tuple[str, str], ...]

CHECKED = False
"""Whether `filled` renders every call afresh too and refuses a template whose filled text differs, as the test suite's `--check-templates` sets it."""


def text(rel: DuckDBPyRelation) -> str:
    """Return the statement `rel` runs, its values written in, as a view is created of."""
    return _render(rel, literals=True, wrap_text=True)[0]


def typed(rel: DuckDBPyRelation) -> Typed:
    """`rel`'s column names and types."""
    return tuple(zip(rel.columns, map(str, rel.types), strict=True))


@dataclass(frozen=True)
class Template:
    """A statement with a slot per relation it reads, which `filled` writes a relation's text into.

    Attributes
    ----------
    pieces
        The text around the slots, one piece more than there are slots.
    slots
        The slots' names, in the order they appear in the text, a name once
        per place its relation is read.
    """

    pieces: tuple[str, ...]
    slots: tuple[str, ...]

    def filled(self, **parts: DuckDBPyRelation) -> str:
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
        return "".join(p + t for p, t in zip(self.pieces, [*slotted, ""], strict=True))


def template(
    con: DuckDBPyConnection,
    build: Callable[..., DuckDBPyRelation],
    **slots: DuckDBPyRelation,
) -> Template:
    """`build` called with each of `slots` read behind a subquery of its own, rendered as a `Template` its texts are cut out of.

    The subquery, aliased by the slot's name, is a boundary the renderer
    neither merges across nor lifts a common table expression out of, so
    each relation's text survives whole. A slot's text inside another's,
    which was filled from it, goes with that one. `build` must hold to the
    contract `filled` states.

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
    whole = text(build(**{n: con.sql(b) for n, b in bounded.items()}))
    for n in (n for n, p in slots.items() if _scanned(p)):
        whole = whole.replace(_INLINED + bounded[n], _CHOSEN + bounded[n])
    found = sorted(
        (at + len(_OPEN), -len(texts[n]), n)
        for n, b in bounded.items()
        for at in _found(whole, b)
    )
    pieces, order, at = [], [], 0
    for start, minus, name in found:
        if start - minus <= at:
            continue
        if start < at:
            msg = f"slot {name!r} overlaps another in the template"
            raise RuntimeError(msg)
        pieces.append(whole[at:start])
        order.append(name)
        at = start - minus
    pieces.append(whole[at:])
    if set(order) != slots.keys():
        msg = "a slot's text appears nowhere in the template but inside another's"
        raise RuntimeError(msg)
    return Template(tuple(pieces), tuple(order))


def filled(
    templates: MutableMapping[Hashable, Template],
    key: Hashable,
    con: DuckDBPyConnection,
    build: Callable[..., DuckDBPyRelation],
    **slots: DuckDBPyRelation,
) -> str:
    """`build` over `slots`, its text that of the template `templates` keeps by `key` and the slots' types, built on a miss.

    `build` must hold to a contract, which `CHECKED` tests:

    - its text depends on `key` and on each slot's columns, types and
      whether it is a table or held rows alone;
    - every value that varies between calls with one key is in a slot,
      never written into the text;
    - it runs nothing, so a fetch or a materialisation is a statement of
      its own, over the template's result;
    - each slot's text survives whole in its own, read once or more
      (`template` refuses one that does not).

    Raises
    ------
    AssertionError
        Where `CHECKED` is set and `build` breaks the contract.
    """
    held = tuple((n, typed(p), _scanned(p)) for n, p in sorted(slots.items()))
    full = (key, held)
    found = templates.get(full)
    if found is None:
        found = templates[full] = template(con, build, **slots)
    elif CHECKED:
        new, old = _canonical(template(con, build, **slots)), _canonical(found)
        at = next(
            (i for i, (a, b) in enumerate(zip(new, old, strict=False)) if a != b), 0
        )
        assert new == old, (
            f"template {key!r} varies: {new[at - 200 : at + 200]!r} "
            f"where the kept one reads {old[at - 200 : at + 200]!r}"
        )
    return found.filled(**slots)


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


def _canonical(found: Template) -> str:
    """`found`'s pieces and slots, each random relation alias numbered in order of appearance."""
    slots = "".join(f"<{s}>" for s in found.slots)
    whole = "\0".join([*found.pieces, slots])
    seen: dict[str, str] = {}
    return _ALIAS.sub(lambda m: seen.setdefault(m[0], f"_alias{len(seen)}"), whole)
