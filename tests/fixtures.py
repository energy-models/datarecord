# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Hand-built layers, written beside the write path rather than through it.

Notes
-----
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from collections import defaultdict
from collections.abc import Sequence
from itertools import product
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from datarecord.store.files import Files, Version
from datarecord.store.store import Revision, Store

TYPES = (
    "Bus",
    "Carrier",
    "Generator",
    "Line",
    "Link",
    "Load",
    "Process",
    "StorageUnit",
    "Store",
    "Transformer",
    "Widget",
)
"""The `ctype` labels `schema()` declares - every one a test writes."""

# No `ctype`: an attribute row is keyed by `name`, unique across every type
# (https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim). The entity tables below keep it.
LONG_COLUMNS = [
    "entity",
    "bus",
    "snapshot",
    "scenario",
    "period",
    "attribute",
    "breakpoint",
    "value",
]


def write_attribute(
    layer: str,
    attribute: str,
    rows: list[dict],
    *,
    snapshot_dtype="datetime64[ns]",
    connection: bool | None = None,
) -> None:
    """Write `attributes/<attribute>.parquet` in the long schema.

    Each row needs at least `name` and `value`; missing dimension columns
    default to NULL, i.e. "applies to the whole axis".
    A `bus` on any row marks a per-connection attribute, its `entity` and `bus`
    packed into the one `connection` struct column - unless `connection` says
    otherwise, for an attribute over the `bus` dim itself; `breakpoint` marks a
    piecewise-linear one; neither is the ordinary component-level scalar.

    Write every file of a layer before its first read: a read freezes the
    layer for its workspace, as a layer is write-once.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
    - [attributes over a multi-dim](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
    """
    df = pd.DataFrame(rows)
    if connection is None:
        connection = "bus" in df
    df["attribute"] = attribute
    for col in LONG_COLUMNS:
        if col not in df:
            df[col] = None
    df["snapshot"] = pd.Series(df["snapshot"]).astype(snapshot_dtype)
    df["scenario"] = df["scenario"].astype("string")
    df["entity"] = df["entity"].astype("string")
    df["bus"] = df["bus"].astype("string")
    df["period"] = df["period"].astype("Int64")
    df["breakpoint"] = df["breakpoint"].astype("float64")
    df["value"] = df["value"].astype("float64")

    target = Path(layer, "attributes")
    target.mkdir(parents=True, exist_ok=True)
    rel = duckdb.connect().from_df(df[LONG_COLUMNS])
    if connection:
        rel = rel.project(
            "{'entity': entity, 'bus': bus} AS connection, * EXCLUDE (entity, bus)"
        )
    rel.to_parquet(str(target / f"{attribute}.parquet"))


def _hashable(value: Any) -> Any:
    """A key value as a dict key: a struct's fields as a tuple."""
    return tuple(value.values()) if isinstance(value, dict) else value


def _as_written(revision: Revision, kind: str, name: str) -> pd.DataFrame:
    """The layer's file of `name` so far, else its parent's resolved one, else nothing.

    A layer restates an axis whole, so a first write in a child starts
    from what its parent resolves, properties and order included.
    """
    path = Path(revision.store.layer_dir(revision.id), kind, f"{name}.parquet")
    if path.exists():
        return pd.read_parquet(path)
    if revision.parent is None:
        return pd.DataFrame()
    record = revision.store.revision(revision.parent).record
    if name in record.schema.properties:
        rel = record.properties.get(name)
    else:
        rel = record.dims.get(name)
    if rel is None:
        return pd.DataFrame()
    frame = rel.df()
    enums = [c for c, t in frame.dtypes.items() if isinstance(t, pd.CategoricalDtype)]
    return frame.astype(dict.fromkeys(enums, object))


def restate(
    revision: Revision,
    kind: str,
    name: str,
    rows: list[dict],
    *,
    drop: Sequence[Any] = (),
) -> None:
    """Write `{kind}/{name}.parquet` of `revision`'s layer whole, as a layer restates it.

    It starts from `_as_written`. A row of `rows` replaces the row of its key
    in place, a new key is appended, and a key in `drop` is left out.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    old = _as_written(revision, kind, name)
    records = old.to_dict("records")
    at = {_hashable(r[name]): i for i, r in enumerate(records)}
    for row in rows:
        key = _hashable(row[name])
        if key in at:
            records[at[key]] = row
        else:
            at[key] = len(records)
            records.append(row)
    gone = {_hashable(k) for k in drop}
    kept = [r for r in records if _hashable(r[name]) not in gone]
    columns = list(dict.fromkeys([*old.columns, *(c for r in rows for c in r)]))
    frame = pd.DataFrame(kept, columns=columns)
    target = Path(revision.store.layer_dir(revision.id), kind)
    target.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target / f"{name}.parquet", index=False)


def write_multi_dim(revision: Revision, multi: str, rows: list[dict]) -> None:
    """Restate `dims/<multi>.parquet` with `rows`, each keyed by the struct column, a dict.

    Notes
    -----
    - [where the rows live](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    """
    restate(revision, "dims", multi, rows)


def unpacked(rel: Any, multi: str = "connection") -> pd.DataFrame:
    """A relation keyed by a multi-dim as pandas, its struct column spread flat.

    A numpy-backed pandas frame has no struct dtype, so the unnest happens on
    the DuckDB side.
    """
    return rel.project(f"unnest({multi}), * EXCLUDE ({multi})").df()


def _connection(row: dict) -> dict:
    """`row` with its `entity` and `bus` packed into the `connection` struct."""
    rest = {k: v for k, v in row.items() if k not in ("entity", "bus")}
    return {"connection": {"entity": row["entity"], "bus": row["bus"]}, **rest}


def write_connections(revision: Revision, rows: list[dict]) -> None:
    """Restate `dims/connection.parquet` with `rows`, each needing `entity` and `bus`.

    No component type - one file holds every type's rows.

    Notes
    -----
    - [attributes over a multi-dim](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
    - [where the rows live](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    """
    restate(revision, "dims", "connection", [_connection(r) for r in rows])


def drop_connections(revision: Revision, pairs: list[tuple[str, str]]) -> None:
    """Restate `dims/connection.parquet` without the `(entity, bus)` pairs.

    Notes
    -----
    - [attributes over a multi-dim](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
    """
    gone = [{"entity": e, "bus": b} for e, b in pairs]
    restate(revision, "dims", "connection", [], drop=gone)


def write_component(revision: Revision, ctype: str, rows: list[dict]) -> None:
    """Restate `dims/entity.parquet` with `rows`, each an entity of type `ctype`."""
    restate(revision, "dims", "entity", [{**r, "ctype": ctype} for r in rows])


def drop_entities(revision: Revision, names: list[str]) -> None:
    """Restate `dims/entity.parquet` without `names`.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    restate(revision, "dims", "entity", [], drop=names)


def write_axis(revision: Revision, dim: str, rows: list[dict]) -> None:
    """Restate `dims/<dim>.parquet` with `rows`, each carrying `dim` and its properties.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
    """
    restate(revision, "dims", dim, rows)


def write_scenarios(revision: Revision, rows: list[dict]) -> None:
    """Restate `dims/scenario.parquet`; each row needs `scenario`."""
    write_axis(revision, "scenario", rows)


def write_periods(revision: Revision, rows: list[dict]) -> None:
    """Restate `dims/period.parquet`; each row needs `period`."""
    write_axis(revision, "period", rows)


def example(load):
    """A PyPSA example network, loaded offline once it is cached.

    PyPSA asks GitHub for a newer release on every netCDF import, without a
    timeout, so a stalled connection hangs the suite. Network requests are
    allowed only to download an example not cached yet, which PyPSA refuses
    with a `ValueError` while they are off.
    """
    import pypsa

    with pypsa.option_context("general.allow_network_requests", False):
        try:
            return load()
        except ValueError:
            pass
    return load()


def rename_components(n, ctype: str, suffix: str) -> None:
    """Suffix one type's member names, in `static` and every dynamic container.

    PyPSA's example networks scope names per component type - a `Load` named
    after its `Bus`, a `Generator` after its `Carrier` - which a record cannot
    represent, names being unique across types.
    `PyPSA.to_datarecord` rejects such a network rather than renaming it,
    so the suffix here is the test suite standing in for the caller that has to
    reconcile the two vocabularies.

    Both containers, because they are keyed by the same names: renaming only
    `static` would orphan every dynamic column, and so silently drop that
    attribute from the record. A stochastic network is keyed by
    `(scenario, name)`, so only the `name` level moves.

    The renamed level is cast back to the dtype it had: `rename` yields an
    `object` index where PyPSA's own is `str`, and `assert_networks_equal`
    compares index dtypes exactly - so without this the helper, not the code
    under test, would fail the round-trip.

    Notes
    -----
    - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    c = n.c[ctype]
    index = c.static.index
    nested = "name" in (index.names or []) and index.nlevels > 1
    if nested:
        level = index.get_level_values("name")
        renamed = {name: f"{name}{suffix}" for name in level}
        c.static.rename(index=renamed, level="name", inplace=True)
        c.static.index = c.static.index.set_levels(
            c.static.index.levels[index.names.index("name")].astype(level.dtype),
            level="name",
        )
        # Per *level*, which is what `rename` does on a MultiIndex - a
        # tuple-keyed mapping matches nothing and silently leaves the columns
        # pointing at names `static` no longer has.
        for frame in c.dynamic.values():
            frame.rename(columns=renamed, level="name", inplace=True)
        return
    renamed = {name: f"{name}{suffix}" for name in index}
    c.static.rename(index=renamed, inplace=True)
    c.static.index = c.static.index.astype(index.dtype)
    for frame in c.dynamic.values():
        frame.rename(columns=renamed, inplace=True)


def empty_root(store: Store) -> Revision:
    """A new root revision, for a test to hand-write its layer into.

    No public API makes an empty revision; this is the store's private
    revision-file write. Its layer holds nothing, the axis of a dim with `one_of`
    included: a label a property names is one a test adds.

    Write every file of a layer before its first read: a read freezes the
    layer for its workspace, as a layer is write-once.
    """
    return store._create(None)


def empty_child(parent: Revision) -> Revision:
    """A new child of `parent` with no layer, like `empty_root`.

    Write every file of a layer before its first read: a read freezes the
    layer for its workspace, as a layer is write-once.
    """
    return parent.store._create(parent.id)


def export_network(n, store: Store) -> Revision:
    """Write `n` as a new root of `store`, through the record layer's own writer.

    Not `n.export_to_parquet`: that emits PyPSA's upstream manifest format,
    which is a different vocabulary from the schema a record declares.
    Going through `store.root` means a test record is written exactly as
    `blocks` writes one.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
    """
    from datarecord.tools.pypsa import PyPSA

    return store.root(PyPSA.to_datarecord(n, store.workspace.con))


def write_schema(schema: Schema, store: Store) -> None:
    """Declare the record's one schema, beside the layers.

    Not per layer: a layer holds only data, so this writes the record-level
    `manifest.json` that every layer in the tree is read under.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
    """
    tree = schema.model_copy(update={"id": None})
    store.files.write_text(store.manifest_uri, tree.model_dump_json(), expect=None)


def widened(base: Schema, attribute: str) -> Schema:
    """`base` declaring one more attribute, shaped like its `p_nom`."""
    return Schema(
        **{
            **dict(base),
            "attributes": {**base.attributes, attribute: base.attributes["p_nom"]},
        }
    )


class RivalWrite:
    """`Files` where a rival replaces `uri` right after this writer reads it.

    Two writers racing on the manifest, made deterministic: the read hands back
    a version the rival's write has already made stale, once.

    Notes
    -----
    - [the manifest's compare-and-swap](https://energy-models.github.io/datarecord/design/storage/concurrency/#the-manifests-compare-and-swap)
    """

    def __init__(self, inner: Files, uri: str, text: str) -> None:
        self._inner = inner
        self._uri = uri
        self._text = text
        self.raced = False

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def read_text(self, uri: str) -> tuple[str, Version] | None:
        found = self._inner.read_text(uri)
        if uri == self._uri and not self.raced:
            self.raced = True
            self._inner.write_text(uri, self._text, expect=None)
        return found


def _default_attributes(dims: dict[str, Any], multi_dims: dict[str, dict[str, str]]):
    """The attributes tests write, declared over whichever dims are in play.

    Writing an attribute the schema does not declare is rejected, since its
    `dims` are what say which columns its file carries - so every attribute a
    test writes has to be declared, and these are the ones they write.

    Addressed over every declared dim rather than a narrower set, which is the
    widest shape and so the one that accepts any row a test writes.
    `efficiency` is the exception, being over the `connection` multi-dim where one
    is declared: that is what puts a `connection` struct column on its file.

    `weight` is the other, addressed by `scenario` alone - long rows of
    `attributes/weight.parquet` like any attribute, whatever its `dims`.
    """
    varying = {"entity", *dims}
    connection = "connection" if "connection" in multi_dims else "entity"
    # A dim named like a coordinate of the connection would clash with its keyword.
    per_connection = set(dims) - set(multi_dims.get(connection, ()))
    declared = {
        "p_nom": AttributeSpec(dtype="float", dims=varying),
        "e_nom": AttributeSpec(dtype="float", dims=varying),
        "p_max_pu": AttributeSpec(dtype="float", dims=varying),
        "p_min_pu": AttributeSpec(dtype="float", dims=varying),
        "marginal_cost": AttributeSpec(dtype="float", dims=varying, breakpoints=True),
        "efficiency": AttributeSpec(dtype="float", dims={connection, *per_connection}),
    }
    if "scenario" in dims:
        declared["weight"] = AttributeSpec(
            dtype="float",
            dims={"scenario"},
            description="How much this scenario counts in the expectation.",
        )
    return declared


def schema(
    *,
    partial: set[str] = {"scenario", "connection"},
    attributes: dict[str, dict[str, AttributeSpec]] | None = None,
    dims: dict[str, Any] = {
        "snapshot": "datetime",
        "period": "int",
        "scenario": "str",
    },
    multi_dims: dict[str, dict[str, str]] = {
        "connection": {"entity": "entity", "bus": "bus"}
    },
) -> Schema:
    """A schema shaped like the PyPSA records most tests build on.

    Defaults match `PyPSA.to_datarecord`: the `entity` axis carrying the
    `ctype` property, which refers to the dim `ctype` listing `TYPES`, a
    `connection` multi-dim over
    `(entity, bus)`, and three declared dims. Override `partial` to pin a
    different layering granularity, `dims` to declare another axis by its
    dtype or as a `Dimension`, `multi_dims` to
    declare a different sparse relation.

    A multi-dim's coordinates are declared dims. `entity` is always `partial`, so
    a layer patches one component's value without restating the rest, and
    `connection` is by default, as the PyPSA tool declares it, so the same
    holds per connection.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
    """
    # Callers declare per type, which is how a modelling framework thinks; the
    # schema stores one spec per attribute, record-wide. Flattening here keeps
    # the tests readable and is exactly what a tool does on the way in.
    flat: dict[str, AttributeSpec] = {}
    for attrs in (attributes or {}).values():
        for attr, spec in attrs.items():
            flat.setdefault(attr, spec)
    # Declared whether or not a caller named them: a test writing `p_max_pu`
    # needs it declared.
    for attr, spec in _default_attributes(dims, multi_dims).items():
        flat.setdefault(attr, spec)
    # A multi-dim's coordinates are dims like any other, so they are declared
    # here rather than assumed - which is what lets a caller pass a multi-dim
    # over coordinates that are not called `bus`.
    coordinates = {c for over in multi_dims.values() for c in over}
    declared = {
        "entity": "str",
        **{c: "str" for c in coordinates},
        **dims,
    }
    return Schema(
        dimensions={
            **{
                d: t if isinstance(t, Dimension) else Dimension(dtype=t)
                for d, t in declared.items()
            },
            "ctype": Dimension(one_of=TYPES),
            **{m: Dimension(over=over) for m, over in multi_dims.items()},
        },
        properties={"entity": {"ctype": PropertySpec(one_of="ctype")}},
        attributes=flat,
        # The default names `connection`, which a caller's own `multi_dims` may drop.
        partial=frozenset(
            p for p in {"entity", *partial} if p in declared or p in multi_dims
        ),
    )


def relation(revision, attribute: str):
    """The resolved long relation for one attribute, as a DuckDB relation.

    A test helper rather than a `Revision` method: `Revision` presents its data
    through `.record` (a `Record`), and a DuckDB-shaped accessor beside it would
    duplicate `record.attributes[attr]`. Tests want relations because they
    assert on `.df()`, so the affordance lives here.
    """
    return revision.record.attributes[attribute]


def broadcast(record, attribute: str, *dims: str) -> dict[tuple, Any]:
    """`attribute`'s value per full coordinate over `dims`, by the broadcast rule.

    A row reaches every label of a dim it leaves NULL, or every tuple of a
    multi-dim, which reads as the tuple of its fields. At each coordinate the row
    naming the union of the dims every row reaching it names wins; a coordinate
    where no single row names that union fails the test.
    """

    def label(value: Any) -> Any:
        return tuple(value.values()) if isinstance(value, dict) else value

    def axis(d: str) -> duckdb.DuckDBPyRelation:
        return record.dims[d]

    labels = {d: [label(c) for (c,) in axis(d).project(d).fetchall()] for d in dims}
    reaching: dict[tuple, list[tuple[frozenset, Any]]] = defaultdict(list)
    for *raw, value in record.attributes[attribute].project(*dims, "value").fetchall():
        coords = [label(c) for c in raw]
        named = frozenset(d for d, c in zip(dims, coords, strict=True) if c is not None)
        reach = [
            labels[d] if c is None else [c] for d, c in zip(dims, coords, strict=True)
        ]
        for full in product(*reach):
            reaching[full].append((named, value))
    resolved = {}
    for full, rows in reaching.items():
        union = frozenset().union(*(named for named, _ in rows))
        winners = [value for named, value in rows if named == union]
        assert len(winners) == 1, f"at {full} no one row names {set(union)}: {rows}"
        resolved[full] = winners[0]
    return resolved
