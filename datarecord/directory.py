"""`DirectoryRecord`, a plain parquet directory as a `Record`.

Framework-independent, like the rest of `datarecord`: hands over narwhals
frames and names no modelling framework.

Notes
-----
- [the record format](https://energy-models.github.io/datarecord/design/format/)
- [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

import narwhals as nw
from duckdb import ColumnExpression as col

from datarecord.duck import fn, try_read_parquet
from datarecord.layered.resolve import read_json, read_schema, with_columns
from datarecord.record import EMPTY, Flags, LazyFrames
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
    def components(self) -> LazyFrames:
        return self._by_type("dims/components")

    @cached_property
    def groups(self) -> Mapping[str, LazyFrames]:
        """Each declared group's rows, keyed by group then by component type.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return {g: self._by_type(f"dims/{g}") for g in self.schema.groups}

    @cached_property
    def attributes(self) -> LazyFrames:
        return self._by_attribute("inputs")

    @cached_property
    def outputs(self) -> LazyFrames:
        return self._by_attribute("outputs")

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
        members = self._read(f"dims/components/{ctype}.parquet")
        result: dict[str, Flags] = {}
        if rel is not None and members is not None:
            # Only the dims a NULL broadcasts over, as the fold's flags are:
            # "did a row set this" is not a question about `entity` or a
            # group's coordinate, which address the row rather than expanding.
            declared = self.schema.broadcast_dims
            # Materialised as NULL where no file carries the column, so the
            # aggregate binds over one relation: `union_by_name` already does
            # this for a dim *some* file has, and this covers a dim none does.
            # Scoping to each attribute's own coordinates then happens below,
            # where the attribute is known (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
            rel = with_columns(self.schema, rel, *declared)
            dims = declared
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
                        *(fn.bool_or(col(d).isnotnull()).alias(f"v_{d}") for d in dims),
                        *(fn.bool_or(col(d).isnull()).alias(f"b_{d}") for d in dims),
                        fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
                    ]
                )
                .fetchall()
            )
            n = len(dims)

            def scope(attribute: str) -> set[str]:
                """Which of `dims` this attribute is actually addressed by.

                One relation covers every attribute, so a dim another uses reads
                NULL here - which must not be reported as "every row broadcasts
                over it" when the attribute has no such axis at all. Falls back
                to all of them for an attribute the schema does not declare,
                whose shape is not the schema's to say.
                """
                own = set(self.schema.coordinates_of(attribute))
                return own & set(dims) if own else set(dims)

            result = {
                r[0]: Flags(
                    frozenset(
                        d
                        for d, on in zip(dims, r[1 : 1 + n], strict=True)
                        if on and d in scope(r[0])
                    ),
                    frozenset(
                        d
                        for d, on in zip(dims, r[1 + n : 1 + 2 * n], strict=True)
                        if on and d in scope(r[0])
                    ),
                    bool(r[1 + 2 * n]),
                )
                for r in rows
            }
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

    def _by_type(self, subdir: str) -> LazyFrames:
        """Keys from the `<Type>.parquet` files present in `subdir`."""
        rel = self._read(f"{subdir}/*.parquet", union_by_name=True)
        if rel is None:
            return EMPTY
        rows = rel.project("component_type").distinct().order("component_type")
        types = tuple(r[0] for r in rows.fetchall())
        return LazyFrames(
            types,
            lambda ctype: nw.from_native(self._require(f"{subdir}/{ctype}.parquet")),
        )

    def _by_attribute(self, subdir: str) -> LazyFrames:
        """Keys from the `<attr>.parquet` files present in `subdir`.

        Read from the `attribute` column rather than by listing filenames, so
        one code path serves a local directory and a remote prefix alike.
        """
        rel = self._read(f"{subdir}/*.parquet", union_by_name=True)
        if rel is None:
            return EMPTY
        rows = rel.project("attribute").distinct().order("attribute")
        names = tuple(r[0] for r in rows.fetchall())
        return LazyFrames(
            names,
            lambda attr: nw.from_native(self._require(f"{subdir}/{attr}.parquet")),
        )
