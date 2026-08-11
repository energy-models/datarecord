"""`DirectoryStore`, a plain parquet directory as a `Store` (design doc §4, §9.3).

Framework-independent, like the rest of `datarecord`: hands over narwhals
frames and names no modelling framework.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

from datarecord.duck import try_read_parquet
from datarecord.layered.resolve import read_json, read_schema
from datarecord.schema import Schema
from datarecord.store import EMPTY, Flags, LazyFrames, _lazy

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


@dataclass(frozen=True)
class DirectoryStore:
    """A plain parquet directory, as a `Store` (§9.3).

    One store, no overlay: what the files hold is what it presents. Reading a
    single layer directly, or any standard parquet store blocks did not write.

    Unlike `LayeredStore` there is no owner map, so `flags` is aggregated from the
    files - a narrow scan, but a scan (§9.3). Cached per component type, since
    a build asks once per type and the answer cannot change under a read-only
    store.
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
        """This store's own `manifest.json`, else the one beside `con`'s layers (§5.6).

        No fold: a directory is one store, so there is nothing to merge across
        (§8.2). A standalone store carries its own manifest and that is the
        answer. A single *layer* of a layered store does not - its schema lives
        beside `layers/`, one for the whole tree - and a connection is already
        scoped to one such root, so reading one layer directly needs nothing
        supplied. Neither present reads as an empty `Schema`: it describes no
        dims, so it resolves no dimensioned data, which is the honest answer
        rather than a guessed default.
        """
        raw = read_json(self.base + "manifest.json")
        if raw is not None:
            return Schema.model_validate(raw)
        return read_schema(self.con)

    @cached_property
    def dims(self) -> LazyFrames:
        # A dim's axis file is `{dim}s.parquet`, so the declared dims name the
        # files to look for; only those that exist become keys.
        declared = self.schema.dims
        present = tuple(d for d in declared if self._read(f"dims/{d}s.parquet"))
        return LazyFrames(
            present, lambda dim: _lazy(self._require(f"dims/{dim}s.parquet"))
        )

    @cached_property
    def components(self) -> LazyFrames:
        return self._by_type("dims/components")

    @cached_property
    def connections(self) -> LazyFrames:
        return self._by_type("dims/connections")

    @cached_property
    def attributes(self) -> LazyFrames:
        return self._by_attribute("inputs")

    @cached_property
    def outputs(self) -> LazyFrames:
        return self._by_attribute("outputs")

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Aggregated from `inputs/*.parquet`, grouped by component type (§4.3).

        Parquet's footer statistics cannot answer this: `stats_null_count` is
        per row group, not per component type, so a file mixing one type's
        per-timestep rows with another's single row says nothing about either.
        Hence a real aggregate - the dim columns projected, no value pages read.

        Which dims to report on comes from the schema (§5), intersected
        with what the files actually carry: a store may declare a dim no file
        has a column for, and `varies`/`broadcast` describe rows.
        """
        cache: dict[str, dict[str, Flags]] = self._flags_cache  # type: ignore[attr-defined]
        if ctype in cache:
            return cache[ctype]

        rel = self._read("inputs/*.parquet", union_by_name=True)
        result: dict[str, Flags] = {}
        if rel is not None:
            declared = self.schema.dims
            dims = tuple(d for d in declared if d in rel.columns)
            pwl = (
                "bool_or(breakpoint IS NOT NULL)"
                if "breakpoint" in rel.columns
                else "false"
            )
            projections = ", ".join(
                [
                    *(f'bool_or("{d}" IS NOT NULL) AS "v_{d}"' for d in dims),
                    *(f'bool_or("{d}" IS NULL) AS "b_{d}"' for d in dims),
                    f"{pwl} AS breakpoints",
                ]
            )
            rows = self.con.sql(
                f"SELECT attribute, {projections}"
                " FROM rel WHERE component_type = $ctype"
                " GROUP BY attribute",
                params={"ctype": ctype},
            ).fetchall()
            n = len(dims)
            result = {
                r[0]: Flags(
                    frozenset(
                        d for d, on in zip(dims, r[1 : 1 + n], strict=True) if on
                    ),
                    frozenset(
                        d
                        for d, on in zip(dims, r[1 + n : 1 + 2 * n], strict=True)
                        if on
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
            types, lambda ctype: _lazy(self._require(f"{subdir}/{ctype}.parquet"))
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
            names, lambda attr: _lazy(self._require(f"{subdir}/{attr}.parquet"))
        )
