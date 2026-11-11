# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The folded axes and dead labels of a record's layers.

Notes
-----
- [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
- [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, field
from functools import reduce
from typing import TYPE_CHECKING
from uuid import UUID

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression
from duckdb.sqltypes import VARCHAR

from datarecord.duck import FOLD, fn, nested, shared_table, tables_in, values_relation
from datarecord.schema.schema import LAYER_COLUMN, LAYER_UUID_TYPE, Schema

if TYPE_CHECKING:
    from datarecord.resolve.sources import LayerSource
    from datarecord.store.store import Workspace


def _fold_name(workspace: Workspace, revision_id: UUID, table: str) -> str:
    """Table `table` of revision `revision_id`'s fold in the workspace's database, qualified for SQL.

    The tables are `owner_map`, `dim_owners`, and `dims_<dim>` and
    `dead_<dim>`, the dim spelled as declared, which `Schema` holds to a plain
    identifier.

    Notes
    -----
    - [the database's schemas](https://energy-models.github.io/datarecord/reference/store/#the-database)
    """
    return f"{col(workspace.name)}.{nested(FOLD, revision_id)}.{table}"


def _fold_axis(
    seed: DuckDBPyRelation | None,
    files: Sequence[tuple[LayerSource, DuckDBPyRelation]],
    dim: str,
    file: str,
) -> DuckDBPyRelation | None:
    """Return the deepest of `files`, else `seed`: one node's resolved axis.

    The one fold for every axis: a layer that changes
    one restates it whole, so the deepest restatement is the answer, in its
    own row order. `files` is root first. A frozen file holding a label of `dim`
    twice is refused; a staging area's table holds no label twice.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
    - [axis order](https://energy-models.github.io/datarecord/reference/data-model/#dims-and-labels)
    """
    if not files:
        return seed
    source, rel = files[-1]
    if source.frozen:
        refuse_twice(rel, [dim], file)
    return rel


def one_of_axis(schema: Schema, dim: str, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """Return the axis of `dim`, a dim with `one_of`: its labels, in list order.

    Raises
    ------
    ValueError
        Where `dim` has no `one_of`.

    Notes
    -----
    - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
    """
    spec = schema.dimensions[dim]
    if spec.one_of is None:
        msg = f"{dim!r} has no `one_of`, so its axis is not the schema's"
        raise ValueError(msg)
    dtype = schema.duck_type(spec)
    return values_relation(
        con, [dim], [[lit(label).cast(dtype)] for label in spec.one_of]
    )


def refuse_one_of_file(schema: Schema, dims: Iterable[str], where: str) -> None:
    """Refuse an axis file of a dim with `one_of` among `dims`, which `where` holds.

    Raises
    ------
    ValueError
        Naming `where` and the dims.

    Notes
    -----
    - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
    """
    held = sorted(set(dims) & set(schema.one_of_dims))
    if held:
        msg = (
            f"{where} holds an axis file of {held}, which list their labels "
            f"with `one_of`; such a dim's axis is the schema's list, so no "
            f"layer writes one (https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)"
        )
        raise ValueError(msg)


_ROWS_PER_PASS = 100_000_000


def _twin(rel: DuckDBPyRelation, columns: Sequence[str]) -> tuple | None:
    """Return the values of `columns` two rows of `rel` share and their row count, else None.

    A hash table over every distinct combination outgrows memory and spill on a file
    of hundreds of millions of rows, so the rows are bucketed by `hash(columns)`
    into passes of at most `_ROWS_PER_PASS` rows, each grouping 64-bit hashes
    only. A hash held twice is a candidate, confirmed by grouping its few rows
    on the columns themselves, so a hash collision is never reported.
    """
    h = fn.hash(*map(col, columns))
    passes = -(-rel.count("*").fetchall()[0][0] // _ROWS_PER_PASS) or 1
    for p in range(passes):
        bucket = rel.filter(h % lit(passes) == lit(p))
        counted = bucket.aggregate([h.alias("_h"), fn.count_star().alias("_rows")])
        for candidate, _ in counted.filter(col("_rows") > lit(1)).fetchall():
            exact = rel.filter(h == lit(candidate)).aggregate(
                [*map(col, columns), fn.count_star().alias("_rows")]
            )
            twin = exact.filter(col("_rows") > lit(1)).limit(1).fetchall()
            if twin:
                return twin[0]
    return None


def refuse_twice(
    rel: DuckDBPyRelation,
    columns: Sequence[str],
    file: str,
    why: str = "an axis file holds each of its labels once "
    "(https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)",
) -> None:
    """Refuse `file`, read as `rel`, holding two rows that agree on every one of `columns`.

    NULL agrees with NULL.

    Raises
    ------
    ValueError
        Naming the values both rows hold, how many rows hold them, and `why`.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    """
    twin = _twin(rel, columns)
    if twin is None:
        return
    *values, rows = twin
    at = ", ".join(
        f"{c}={v}" for c, v in zip(columns, values, strict=True) if v is not None
    )
    msg = f"{file} holds {rows} rows at {at or 'every coordinate'}; {why}"
    raise ValueError(msg)


def refuse_unheld(
    rows: DuckDBPyRelation,
    schema: Schema,
    columns: Iterable[str],
    held: Callable[[str], DuckDBPyRelation | None],
    where: str,
    *,
    staged: bool,
    subject: str | None = None,
) -> None:
    """Refuse a label or tuple in `rows` that the axis holding it lacks.

    A dim's column of `columns` names labels of that dim, a multi-dim's column
    tuples of that multi-dim and, through each field, labels of the dim it
    draws on, and the column of a property of `subject` that refers to a dim
    names labels of that dim. Any other column is not checked. `subject`'s own
    column holds the labels or tuples being added, so of it only a multi-dim's
    fields are checked.
    Labels are checked before tuples, so a tuple naming an unheld label is
    refused for that label. NULL is no label and passes.

    Parameters
    ----------
    held
        Returns the axis of a name, `None` where there is none:
        as staged where `staged`, else the layer's final one.
    where
        What names the labels, to start the message with.

    Raises
    ------
    KeyError
        Where `staged`, naming the dim and up to three such labels
        or tuples.
    ValueError
        Where not `staged`, naming the same.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
    - [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
    """
    taken = {
        n: d for (s, n), d in schema.referring_properties().items() if s == subject
    }
    labels: dict[str, tuple[Expression, str]] = {}
    fields: dict[str, tuple[Expression, str]] = {}
    tuples: dict[str, tuple[Expression, str]] = {}
    for c in columns:
        if c in taken:
            labels[f"{subject}.{c}"] = (col(c), taken[c])
        elif c in schema.multi_dims:
            fields |= {f"{c}.{f}": (col(c)[f], d) for f, d in schema.over(c).items()}
            if c != subject:
                tuples[c] = (col(c), c)
        elif c in schema.dimensions and c != subject:
            labels[c] = (col(c), c)
    named = labels | fields | tuples
    unheld = _first_unheld(rows, {k: (e, held(n), n) for k, (e, n) in named.items()})
    if unheld is None:
        return
    key, found = unheld
    name = named[key][1]
    multi = name in schema.multi_dims
    shown = (
        f"{name!r} tuples [{', '.join(found)}]" if multi else f"{name!r} labels {found}"
    )
    via = "" if key == name else f" in {key!r}"
    if name in schema.one_of_dims:
        holder = f"its `one_of`, {list(schema.dimensions[name].one_of or ())},"
        remedy = "use one of its labels"
        link = "data-model/#a-dim-with-one_of"
    elif staged:
        holder = "the staged axis"
        remedy = "`add` them first"
        link = "editing/#validation"
    else:
        holder = f"the layer's dims/{name}.parquet"
        remedy = "add them there or drop those rows"
        link = "layers/"
    msg = (
        f"{where} names {shown}{via}, which {holder} does not hold; {remedy} "
        f"(https://energy-models.github.io/datarecord/reference/{link})"
    )
    raise (KeyError if staged else ValueError)(msg)


def _first_unheld(
    rows: DuckDBPyRelation,
    named: Mapping[str, tuple[Expression, DuckDBPyRelation | None, str]],
) -> tuple[str, list[str]] | None:
    """Return the first name in `named` whose labels in `rows` its relation lacks, with up to three of them.

    `named` maps a name to the expression over `rows` giving a label or a
    tuple, the axis relation that holds them, and that relation's
    column. A `None` relation holds nothing. NULL is no label and
    passes. The labels come back as text, in sorted order. `rows` is
    anti-joined against every relation in one query.
    """
    parts = []
    for i, (label, held, column) in enumerate(named.values()):
        part = rows.project(label.alias("_label")).filter(col("_label").isnotnull())
        if held is not None:
            part = part.set_alias("r").join(
                held.project(col(column).alias("_held")).set_alias("h"),
                col("r", "_label") == col("h", "_held"),
                how="anti",
            )
        parts.append(
            part.project(
                lit(i).alias("_i"), col("_label").cast(VARCHAR).alias("_label")
            )
        )
    if not parts:
        return None
    first = (
        reduce(DuckDBPyRelation.union, parts)
        .aggregate("_i, list(DISTINCT _label ORDER BY _label)[1:3] AS _labels", "_i")
        .order("_i")
        .limit(1)
        .fetchall()
    )
    if not first:
        return None
    return list(named)[first[0][0]], first[0][1]


def removed_labels(
    before: DuckDBPyRelation, now: DuckDBPyRelation, dim: str
) -> DuckDBPyRelation:
    """The labels of `dim` the axis `before` holds and the axis `now` does not."""
    return (
        before.project(col(dim))
        .set_alias("b")
        .join(
            now.project(col(dim)).set_alias("n"),
            col("b", dim) == col("n", dim),
            how="anti",
        )
    )


def _dead_labels(
    held: Sequence[DuckDBPyRelation],
    final: DuckDBPyRelation,
    dim: str,
    *,
    fixed: bool,
) -> DuckDBPyRelation | None:
    """Return the labels of `held` that `final` lacks, `None` where there is none.

    `held` is every set of labels the dim had: the base's dead labels
    and resolved axis, and each restatement above the base. A label one of
    them holds and the final axis lacks is one some layer removed. `None`
    rather than an empty relation, so `_living` adds no join for it, unless
    `fixed`: a restatement that can still change is answered by the
    anti-join itself, empty or not, so no plan depends on its rows.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    labels = [rel.project(col(dim)) for rel in held]
    union = reduce(DuckDBPyRelation.union, labels).distinct().set_alias("h")
    dead = union.join(
        final.project(col(dim)).set_alias("f"),
        col("h", dim) == col("f", dim),
        how="anti",
    )
    if fixed:
        return dead
    if not dead.limit(1).fetchall():
        return None
    return dead


def _base_and_above[B](
    sources: Sequence[LayerSource], at_hand: Callable[[LayerSource], B | None]
) -> tuple[B | None, list[LayerSource]]:
    """Split `sources` at the deepest one whose fold is `at_hand`: that fold, then the rest.

    A fold is at hand where the source is materialised, or where the workspace
    holds its node's fold as shared tables (`Axes.held`, `Fold.held`). It is the
    fold's base - already folded over everything at or below it - and only the
    sources above it are folded on top. No fold at hand means no base and the
    whole list folds from the root.

    The last source is never the base: its own fold would return the answer
    already folded and fold nothing on top, the reason `sources_to_read` truncates only
    at proper ancestors.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    for i in range(len(sources) - 2, -1, -1):
        base = at_hand(sources[i])
        if base is not None:
            return base, list(sources[i + 1 :])
    return None, list(sources)


def _axes_at_hand(source: LayerSource) -> Axes | None:
    """`source`'s node's axes, materialised or held, `None` where neither."""
    fold = source.materialised()
    return Axes.held(source) if fold is None else fold.axes


def mortal(schema: Schema) -> tuple[str, ...]:
    """The dims a fold keeps the dead labels of.

    One outside the owner map's index, whose values naming a dead label only the
    read can drop (`_living`), and a dim a multi-dim's tuples range over or a
    property refers to, which the writer checks them against
    (`write._require_living_references`). An index dim's values the fold
    drops itself (`fold_layer`). A dim with `one_of` has no dead labels,
    its axis being the schema's list.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    drawn = {
        *(d for m in schema.multi_dims for d in schema.over(m).values()),
        *schema.referring_properties().values(),
    }
    outside_index = set(schema.dimensions) - set(schema.partial_dims)
    kept = (drawn | outside_index) - set(schema.one_of_dims)
    return tuple(c for c in schema.dimensions if c in kept)


@dataclass(frozen=True)
class Axes(Mapping[str, DuckDBPyRelation]):
    """A record's folded axes, by dim, with the schema, owners and dead labels they fold under.

    They travel together because the fold needs both wherever it broadcasts a
    NULL against "all values of that dim". `axes[dim]` is the dim's axis
    relation, and a dim with no axis raises `KeyError`.

    Parameters
    ----------
    schema : Schema
        The record's schema.
    relations : dict of str to DuckDBPyRelation
        Each declared dim's folded axis relation, full row rather than the dim's
        column alone - its properties ride along; a multi-dim's column is
        its struct column. A dim with no rows anywhere is absent rather than
        present-and-empty.
    dim_owners : dict of str to UUID
        Each folded axis's owner: the layer whose `dims/<dim>.parquet` it
        resolved to. A dim with `one_of` has none, its axis being the schema's.
    dead : dict of str to DuckDBPyRelation
        Each `mortal` dim's dead labels, keyed as its axis.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [the schema](https://energy-models.github.io/datarecord/reference/schema/)
    """

    schema: Schema
    relations: dict[str, DuckDBPyRelation]
    dim_owners: dict[str, UUID]
    dead: dict[str, DuckDBPyRelation] = field(default_factory=dict)

    def __getitem__(self, dim: str) -> DuckDBPyRelation:
        return self.relations[dim]

    def __iter__(self) -> Iterator[str]:
        return iter(self.relations)

    def __len__(self) -> int:
        return len(self.relations)

    @classmethod
    def from_rows(
        cls,
        schema: Schema,
        rows: Sequence[tuple[str, UUID, bool]],
        axis: Callable[[str, UUID], DuckDBPyRelation],
        dead: Callable[[str], DuckDBPyRelation],
    ) -> Axes:
        """The axes `(dim, layer_uuid, has_dead)` rows (`owners`) name.

        Parameters
        ----------
        axis
            Opens a dim's folded axis, given the dim and its owner.
        dead
            Opens a dim's dead labels, called only where its row has any.
        """
        return cls(
            schema=schema,
            relations={d: axis(d, layer) for d, layer, _ in rows},
            dim_owners={d: layer for d, layer, _ in rows},
            dead={d: dead(d) for d, _, has_dead in rows if has_dead},
        )

    @classmethod
    def tabled(
        cls,
        revision_id: UUID,
        schema: Schema,
        rows: Sequence[tuple[str, UUID, bool]],
        workspace: Workspace,
    ) -> Axes:
        """The axes `revision_id`'s shared tables hold, given its `dim_owners` rows.

        A `one_of` dim's axis is the schema's, so it is not among them.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        """
        con = workspace.con
        return cls.from_rows(
            schema,
            rows,
            lambda d, _: con.table(_fold_name(workspace, revision_id, f"dims_{d}")),
            lambda d: con.table(_fold_name(workspace, revision_id, f"dead_{d}")),
        )

    @classmethod
    def held(cls, source: LayerSource) -> Axes | None:
        """`source`'s node's axes from the workspace's shared tables, `None` where they lack them.

        The tables are keyed by the node a `Record` reads, and hold the fold over
        its whole ancestry, so they stand for the source only as the last of its
        own ancestry - as every proper ancestor in a source list is.
        """
        rid, ws = source.layer_id, source.workspace
        if "dim_owners" not in tables_in(ws.con, ws.name, FOLD, rid):
            return None
        rows = ws.con.table(_fold_name(ws, rid, "dim_owners")).fetchall()
        return cls.tabled(rid, source.schema, rows, ws)

    @classmethod
    def shared(cls, revision_id: UUID, sources: Sequence[LayerSource]) -> Axes:
        """The axes of the frozen `sources`, kept as shared tables and read on the calling thread's cursor.

        Each axis and dead label set is a table (`_fold_name`), built by
        whichever cursor misses it first; `dim_owners` is created after
        them, so a reader finding it finds every table it names. A `one_of`
        dim's axis is the schema's, carried so a fold onto these axes binds
        no `VALUES` list per read.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
        """
        schema, ws = sources[-1].schema, sources[-1].workspace
        con = ws.con

        def build() -> DuckDBPyRelation:
            axes = cls.resolve(sources)
            made = [
                *(("dims", dim, axes[dim]) for dim in axes.dim_owners),
                *(("dead", name, rel) for name, rel in axes.dead.items()),
            ]
            for kind, name, rel in made:
                table = f"{kind}_{name}"
                shared_table(
                    con, ws.name, FOLD, revision_id, table, lambda: nullcontext(rel)
                )  # noqa: B023 - called before `rel` moves on
            return axes.owners(con)

        index = shared_table(
            con, ws.name, FOLD, revision_id, "dim_owners", lambda: nullcontext(build())
        )
        rows = index.fetchall()
        tabled = cls.tabled(revision_id, schema, rows, ws)
        relations = tabled.relations | {
            n: one_of_axis(schema, n, con) for n in schema.one_of_dims
        }
        return dataclasses.replace(
            tabled,
            relations={n: relations[n] for n in schema.dimensions if n in relations},
        )

    @classmethod
    def resolve(
        cls,
        sources: Sequence[LayerSource],
        split: tuple[Axes | None, list[LayerSource]] | None = None,
    ) -> Axes:
        """Resolve every dim the last source's schema declares and the dead labels.

        Parameters
        ----------
        sources : sequence of LayerSource
            Root first, ending in the layer being resolved. A source that is not
            frozen, a staging area, restates every axis but those with `one_of`.
        split
            A folded seed and the sources to fold onto it, where the caller
            already holds them: `_base_and_above`, or the frozen
            layers' axes and the sources above them.

        Raises
        ------
        ValueError
            If the deepest file of an axis holds one label twice.

        Notes
        -----
        - [the schema](https://energy-models.github.io/datarecord/reference/schema/)
        - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
        - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
        """
        schema, con = sources[-1].schema, sources[-1].workspace.con
        base, above = split or _base_and_above(sources, _axes_at_hand)
        restated = {d for d in schema.dimensions if d not in schema.one_of_dims}
        held_files = [source.axes() if source.frozen else restated for source in above]
        for source, names in zip(above, held_files, strict=True):
            refuse_one_of_file(schema, names, f"layer {source.layer_id}")
        keep = mortal(schema)
        relations: dict[str, DuckDBPyRelation] = {}
        dead: dict[str, DuckDBPyRelation] = {}
        dim_owners: dict[str, UUID] = {}
        for name in schema.dimensions:
            seed = None if base is None else base.get(name)
            if name in schema.one_of_dims:
                relations[name] = (
                    one_of_axis(schema, name, con) if seed is None else seed
                )
                continue
            files = [
                (s, rel)
                for s, held in zip(above, held_files, strict=True)
                if name in held and (rel := s.axis(name)) is not None
            ]
            rel = _fold_axis(seed, files, name, f"dims/{name}.parquet")
            if rel is None:
                continue
            relations[name] = rel
            if files:
                dim_owners[name] = files[-1][0].layer_id
            elif base is not None:
                dim_owners[name] = base.dim_owners[name]
            if name not in keep:
                continue
            before = None if base is None else base.dead.get(name)
            if files:
                held = [
                    r for r in (before, seed, *(r for _, r in files)) if r is not None
                ]
                fixed = not all(s.frozen for s, _ in files)
                before = _dead_labels(held, rel, name, fixed=fixed)
            if before is not None:
                dead[name] = before
        return cls(schema=schema, relations=relations, dead=dead, dim_owners=dim_owners)

    def owners(self, con: DuckDBPyConnection) -> DuckDBPyRelation:
        """Return `(dim, layer_uuid, has_dead)`, one row per `dim_owners` entry.

        `has_dead` says whether the dim has dead labels; every dim with dead
        labels has an axis, so none is left out.

        Notes
        -----
        - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        """
        return con.sql(
            "SELECT unnest($dims::VARCHAR[]) AS dim, "
            f"unnest($layers::VARCHAR[])::{LAYER_UUID_TYPE} AS {LAYER_COLUMN}, "
            "unnest($dead::BOOLEAN[]) AS has_dead",
            params={
                "dims": list(self.dim_owners),
                "layers": [str(u) for u in self.dim_owners.values()],
                "dead": [d in self.dead for d in self.dim_owners],
            },
        )
