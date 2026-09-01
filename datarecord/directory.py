"""`DirectoryRecord`, a plain parquet directory as a `Record`.

Framework-independent, like the rest of `datarecord`: hands over narwhals
frames and names no modelling framework.

Notes
-----
- [the record format](https://energy-models.github.io/datarecord/design/format/)
- [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

import narwhals as nw
from duckdb import ColumnExpression as col

from datarecord.duck import distinct_values, fn, struct_of, try_read_parquet
from datarecord.layered.resolve import read_json, read_schema, with_columns
from datarecord.record import EMPTY, Flags, LazyFrames, flags_from_rows
from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


@dataclass(frozen=True)
class DirectoryRecord:
    """A plain parquet directory, as a `Record`.

    No overlay: what the files hold is what it presents - one layer read
    directly, or any standard parquet directory. With no owner map, `flags` is a
    scan, cached per component type.

    Notes
    -----
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """

    uri: str
    con: DuckDBPyConnection

    def __post_init__(self) -> None:
        object.__setattr__(self, "_flags_cache", {})

    @property
    def base(self) -> str:
        return self.uri if self.uri.endswith("/") else self.uri + "/"

    @cached_property
    def schema(self) -> Schema:
        """This record's own `manifest.json`, else the one beside `con`'s layers.

        A standalone record carries its own; a single *layer* of a layered record
        does not, and a connection is already scoped to one root, so reading one
        layer directly needs nothing supplied. Neither present reads as an empty
        `Schema`.

        Notes
        -----
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        """
        raw = read_json(self.base + "manifest.json")
        if raw is not None:
            return Schema.model_validate(raw)
        return read_schema(self.con)

    @cached_property
    def dims(self) -> LazyFrames:
        # A dim's axis file is `{dim}.parquet`, so the declared dims name the
        # files to look for; only those that exist become keys.
        declared = self.schema.dims
        present = tuple(d for d in declared if self._read(f"dims/{d}.parquet"))
        return LazyFrames(
            present, lambda dim: nw.from_native(self._require(f"dims/{dim}.parquet"))
        )

    @cached_property
    def entity_types(self) -> LazyFrames:
        return self._keyed_by("dims/entity_type", "entity_type")

    @cached_property
    def groups(self) -> LazyFrames:
        """Each declared group's rows, keyed by group - one file each.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [where the rows live](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        present = tuple(
            g for g in self.schema.groups if self._read(f"groups/{g}.parquet")
        )
        return LazyFrames(
            present,
            lambda group: nw.from_native(self._require(f"groups/{group}.parquet")),
        )

    @cached_property
    def attributes(self) -> LazyFrames:
        return self._keyed_by("inputs", "attribute")

    @cached_property
    def outputs(self) -> LazyFrames:
        return self._keyed_by("outputs", "attribute")

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Aggregated from `inputs/*.parquet`, scoped to one component type.

        A real aggregate, not footer statistics: `stats_null_count` is per row
        group, not per component type, so a file mixing two types' rows says
        nothing about either. Only dim columns are projected.

        Which dims to report on is the schema's, intersected with what the files
        carry - a record may declare a dim no file has a column for.

        Scoped by a semi-join to the type's member file, the entity table for it - so a type with no such file has no attribute rows either.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
        """
        cache: dict[str, dict[str, Flags]] = self._flags_cache  # type: ignore[attr-defined]
        if ctype in cache:
            return cache[ctype]

        rel = self._read("inputs/*.parquet", union_by_name=True)
        members = self._read(f"dims/entity_type/{ctype}.parquet")
        result: dict[str, Flags] = {}
        if rel is not None and members is not None:
            # Only the dims a NULL broadcasts over, as the fold's flags are:
            # "did a row set this" is not a question about `entity` or a
            # group's coordinate, which address the row rather than expanding.
            dims = self.schema.broadcast_dims
            # Materialised as NULL where no file carries the column, so the
            # aggregate binds over one relation: `union_by_name` already does
            # this for a dim *some* file has, and this covers a dim none does.
            # Scoping to each attribute's own coordinates then happens in
            # `flags_from_rows` (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
            rel = with_columns(self.schema, rel, *dims)
            rows = (
                rel.set_alias("i")
                .join(
                    members.project("entity").distinct().set_alias("e"),
                    "i.entity = e.entity",
                    how="semi",
                )
                .aggregate(
                    [
                        col("attribute"),
                        struct_of(
                            {d: fn.bool_or(col(d).isnotnull()) for d in dims}
                        ).alias("varies"),
                        struct_of({d: fn.bool_or(col(d).isnull()) for d in dims}).alias(
                            "broadcast"
                        ),
                        fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
                    ]
                )
                .fetchall()
            )
            result = flags_from_rows(self.schema, dims, rows)
        cache[ctype] = result
        return result

    # -- reads --------------------------------------------------------------

    def _read(self, path: str, **kwargs: object) -> DuckDBPyRelation | None:
        return try_read_parquet(self.base + path, self.con, **kwargs)

    def _require(self, path: str) -> DuckDBPyRelation:
        rel = self._read(path)
        if rel is None:  # pragma: no cover - keys come from what exists
            raise KeyError(path)
        return rel

    def _keyed_by(self, subdir: str, column: str) -> LazyFrames:
        """`subdir`'s files, keyed by the distinct values of `column`.

        Read from a column rather than by listing filenames, so one code path
        serves a local directory and a remote prefix alike: `entity_type` names
        the per-type files under `dims/entity_type/`, `attribute` the per-attribute
        ones under `inputs/` and `outputs/`.
        """
        rel = self._read(f"{subdir}/*.parquet", union_by_name=True)
        if rel is None:
            return EMPTY
        return LazyFrames(
            distinct_values(rel, column),
            lambda key: nw.from_native(self._require(f"{subdir}/{key}.parquet")),
        )
