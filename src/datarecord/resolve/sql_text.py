# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A relation's SQL text, and templates whose slots are filled with it.

DuckDB binds every step of a relation as it is built, so a read built step by
step costs a bind per step. A template is such a read built once over row-less
stand-ins, and filling it writes each relation's text where its stand-in's is:
the statement is bound once, as a whole.

Only this module calls DuckDB's private renderer, so a change to the renderer
breaks `text` alone.

Notes
-----
- [registered views](https://energy-models.github.io/datarecord/reference/format/#registered-views)
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from duckdb import ColumnExpression as col
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb._relation import _render

from datarecord.duck import empty_relation


def text(rel: DuckDBPyRelation) -> str:
    """Return the statement `rel` runs, its values written in, as a view is created of."""
    return _render(rel, literals=True, wrap_text=True)[0]


@dataclass(frozen=True)
class Template:
    """A statement with a slot per relation it reads, which `filled` writes a relation's text into.

    Attributes
    ----------
    pieces
        The text around the slots, one piece more than there are slots.
    slots
        The slots' names, in the order they appear in the text.
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
            msg = f"the template has slots {sorted(self.slots)}, not {sorted(parts)}"
            raise ValueError(msg)
        texts = [text(parts[s]) for s in self.slots]
        return "".join(p + t for p, t in zip(self.pieces, [*texts, ""], strict=True))


def template(
    con: DuckDBPyConnection,
    build: Callable[..., DuckDBPyRelation],
    **slots: DuckDBPyRelation,
) -> Template:
    """`build` called with a row-less stand-in per slot, typed as the relation `slots` gives it, rendered as a `Template`.

    A template fits every relation of the types its slot was built over.

    Raises
    ------
    RuntimeError
        Where a stand-in's text appears other than once in the rendered statement.
    """
    stand_ins = {name: _stand_in(con, name, rel) for name, rel in slots.items()}
    whole = text(build(**{n: con.sql(s) for n, s in stand_ins.items()}))
    found = {n: whole.find(s) for n, s in stand_ins.items()}
    if any(whole.count(s) != 1 for s in stand_ins.values()):
        msg = "a stand-in's text appears other than once in the template"
        raise RuntimeError(msg)
    order = sorted(found, key=found.__getitem__)
    pieces, at = [], 0
    for name in order:
        pieces.append(whole[at : found[name]])
        at = found[name] + len(stand_ins[name])
    pieces.append(whole[at:])
    return Template(tuple(pieces), tuple(order))


def _stand_in(con: DuckDBPyConnection, name: str, rel: DuckDBPyRelation) -> str:
    """The text of a row-less relation typed as `rel`, aliased as the slot `name` so no other stand-in's text equals it."""
    empty = empty_relation(con, **dict(zip(rel.columns, rel.types, strict=True)))
    return f"SELECT * FROM ({text(empty)}) AS {col(f'__slot_{name}')}"
