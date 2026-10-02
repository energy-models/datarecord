# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Hand-built patch layers, and PyPSA example networks as a record's first layer.

Notes
-----
- [sources](https://energy-models.github.io/datarecord/design/sources/)
"""

from pathlib import Path

import narwhals as nw
import pandas as pd

from datarecord.duck import layer_dir
from datarecord.layered.resolve import write_schema as record_write_schema
from datarecord.layered.write import write_record
from datarecord.schema import (
    AttributeSpec,
    Dimension,
    Group,
    Schema,
)
from datarecord.sources import from_sources

# No `entity_type`: an attribute row is keyed by `name`, unique across every type
# (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types). The entity tables below keep it.
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


def write_input(
    layer: str, attribute: str, rows: list[dict], *, snapshot_dtype="datetime64[ns]"
) -> None:
    """Write `inputs/<attribute>.parquet` in the long schema.

    Each row needs at least `name` and `value`; missing dimension columns
    default to NULL, i.e. "applies to the whole axis".
    `bus` set marks a per-connection attribute, `breakpoint`
    a piecewise-linear one; both NULL is the ordinary component-level
    scalar.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    df = pd.DataFrame(rows)
    df["attribute"] = attribute
    for col in LONG_COLUMNS:
        if col not in df:
            df[col] = None
    df["snapshot"] = pd.Series(df["snapshot"]).astype(snapshot_dtype)
    df["scenario"] = df["scenario"].astype("string")
    df["bus"] = df["bus"].astype("string")
    df["period"] = df["period"].astype("Int64")
    df["breakpoint"] = df["breakpoint"].astype("float64")
    df["value"] = df["value"].astype("float64")

    target = Path(layer, "inputs")
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{attribute}.parquet"
    df = df[LONG_COLUMNS]
    if path.exists():
        df = pd.concat([pd.read_parquet(path), df], ignore_index=True)
    df.to_parquet(path, index=False)


def write_group(layer: str, group: str, rows: list[dict]) -> None:
    """Write `groups/<group>.parquet` from plain rows, whatever columns they carry.

    The generic form of `write_connections`: one file per group, keyed by its
    coordinates, with `deleted` supplied where a row does not carry it.

    Notes
    -----
    - [where the rows live](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    df = pd.DataFrame(rows)
    if "deleted" not in df:
        df["deleted"] = False
    df["deleted"] = df["deleted"].fillna(False).astype(bool)
    target = Path(layer, "groups")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / f"{group}.parquet", index=False)


def write_connections(layer: str, rows: list[dict]) -> None:
    """Write `groups/connection.parquet`, including the `deleted` tombstone.

    Each row needs `entity` and `bus`; `role` describes the connection and keys
    nothing, so it is optional here.

    No component type - one file holds every type's rows. Appended rather than
    replaced, since a layer may write them a call at a time.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [where the rows live](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    df = pd.DataFrame(rows)
    for col in ("scenario", "role"):
        if col not in df:
            df[col] = None
        df[col] = df[col].astype("string")
    if "deleted" not in df:
        df["deleted"] = False
    df["deleted"] = df["deleted"].fillna(False).astype(bool)

    lead = ["entity", "bus", "role", "scenario", "deleted"]
    ordered = lead + [c for c in df.columns if c not in lead]
    target = Path(layer, "groups")
    target.mkdir(parents=True, exist_ok=True)
    path = target / "connection.parquet"
    out = df[ordered]
    if path.exists():
        out = pd.concat([pd.read_parquet(path), out], ignore_index=True)
    out.to_parquet(path, index=False)


def tombstone_connection(layer: str, pairs: list[tuple[str, str]]) -> None:
    """Mark connections deleted in this layer, by `(entity, bus)`.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    write_connections(
        layer,
        [{"entity": name, "bus": bus, "deleted": True} for name, bus in pairs],
    )


# The attributes `_default_attributes` declares over more than `entity`: a
# component's constant value of one is a row per entity with every other dim
# NULL, not an entity-axis column.
LONG_ATTRIBUTES = {
    "p_nom",
    "e_nom",
    "p_max_pu",
    "p_min_pu",
    "marginal_cost",
    "efficiency",
}


def write_entity_type(layer: str, ctype: str, rows: list[dict]) -> None:
    """Write components of one type: entity rows, their type, and their constants.

    The entity axis holds membership, tombstones and every other column; the
    `entity_type` group holds each entity's type; a constant of an attribute
    declared over more than `entity` is a broadcast row in `inputs/`.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    df = pd.DataFrame(rows)
    if "deleted" not in df:
        df["deleted"] = False
    df["deleted"] = df["deleted"].fillna(False).astype(bool)
    long = [c for c in df.columns if c in LONG_ATTRIBUTES]
    for attribute in long:
        values = df[["entity", attribute]].dropna()
        if not values.empty:
            write_input(
                layer,
                attribute,
                values.rename(columns={attribute: "value"}).to_dict("records"),
            )
    _append(Path(layer, "dims", "entity.parquet"), df.drop(columns=long))
    kinds = df[["entity", "deleted"]].assign(entity_type=ctype)
    _append(Path(layer, "groups", "entity_type.parquet"), kinds[~kinds["deleted"]])


def _append(path: Path, df: pd.DataFrame) -> None:
    """`df` added to the parquet file at `path`, which several calls share."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        df = pd.concat([pd.read_parquet(path), df], ignore_index=True)
    df.to_parquet(path, index=False)


def tombstone(layer: str, ctype: str, names: list[str]) -> None:
    """Mark components deleted in this layer: an entity-axis tombstone per name.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    _append(
        Path(layer, "dims", "entity.parquet"),
        pd.DataFrame({"entity": names, "deleted": True}),
    )


def members(record, ctype: str) -> pd.DataFrame:
    """One type's entity-axis rows: the entities the `entity_type` group names `ctype`."""
    axis = record.dims["entity"].collect("pandas").to_native()
    kinds = record.groups["entity_type"].collect("pandas").to_native()
    mine = kinds.loc[kinds["entity_type"] == ctype, ["entity"]]
    return axis.merge(mine, on="entity").reset_index(drop=True)


def names(record, ctype: str) -> list[str]:
    """One type's entity names, in entity-axis order - what `flags` is scoped by."""
    return [str(n) for n in members(record, ctype)["entity"]]


def write_scenarios(layer: str, rows: list[dict]) -> None:
    """Write `dims/scenario.parquet`; each row needs `scenario` and `weight`."""
    df = pd.DataFrame(rows)
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "scenario.parquet", index=False)


def write_periods(layer: str, rows: list[dict]) -> None:
    """Write `dims/period.parquet`; each row needs `period`."""
    df = pd.DataFrame(rows)
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "period.parquet", index=False)


def write_snapshots(layer: str, rows: list[dict]) -> None:
    """Write `dims/snapshot.parquet`; each row needs `snapshot`.

    A `period` column makes it a nested axis, keyed by `(period,
    snapshot)` rather than by the timestamp alone.

    Notes
    -----
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    """
    df = pd.DataFrame(rows)
    df["snapshot"] = pd.Series(df["snapshot"]).astype("datetime64[ns]")
    if "period" in df:
        df["period"] = df["period"].astype("Int64")
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "snapshot.parquet", index=False)


def write_axis(layer: str, dim: str, rows: list[dict]) -> None:
    """Write `dims/<dim>.parquet` from plain rows, whatever columns they carry.

    The generic form of `write_scenarios`/`write_periods`: an axis file is its
    key column plus whatever else it holds - a mapping's column, an attribute
    addressed by the axis alone.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(target / f"{dim}.parquet", index=False)


def rename_components(n, ctype: str, suffix: str) -> None:
    """Suffix one type's member names, in `static` and every dynamic container.

    PyPSA's example networks scope names per component type - a `Load` named
    after its `Bus`, a `Generator` after its `Carrier` - which a record cannot
    represent, names being unique across types. The suffix here is the test
    suite standing in for the caller that has to reconcile the two
    vocabularies.

    Both containers, because they are keyed by the same names: renaming only
    `static` would orphan every dynamic column, and so silently drop that
    attribute from the record. A stochastic network is keyed by
    `(scenario, name)`, so only the `name` level moves.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    c = n.c[ctype]
    index = c.static.index
    nested = "name" in (index.names or []) and index.nlevels > 1
    if nested:
        level = index.get_level_values("name")
        renamed = {name: f"{name}{suffix}" for name in level}
        c.static.rename(index=renamed, level="name", inplace=True)
        # Per *level*, which is what `rename` does on a MultiIndex - a
        # tuple-keyed mapping matches nothing and silently leaves the columns
        # pointing at names `static` no longer has.
        for frame in c.dynamic.values():
            frame.rename(columns=renamed, level="name", inplace=True)
        return
    renamed = {name: f"{name}{suffix}" for name in index}
    c.static.rename(index=renamed, inplace=True)
    for frame in c.dynamic.values():
        frame.rename(columns=renamed, inplace=True)


SERIES = ("p_max_pu", "p_min_pu", "marginal_cost")


def network_schema(n) -> Schema:
    """`schema()`, with the network attributes the tests read declared as PyPSA types them.

    `p_nom` and `carrier` are entity-axis columns; `SERIES` and the per-port
    `efficiency` vary over `snapshot`, and over `scenario` too where `n` has
    one; `role` is a column of the `connection` group. The results are the ones
    tests stage.
    """
    varying = {"snapshot", "scenario"} if n.has_scenarios else {"snapshot"}
    declared = {
        "p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
        "carrier": AttributeSpec(dtype=nw.String(), dims={"entity"}),
        **{
            a: AttributeSpec(dtype=nw.Float64(), dims={"entity", *varying})
            for a in SERIES
        },
        "efficiency": AttributeSpec(dtype=nw.Float64(), dims={"connection", *varying}),
        "role": AttributeSpec(dtype=nw.String(), dims={"connection"}),
    }
    results = {
        "p": AttributeSpec(dtype=nw.Float64(), dims={"entity", *varying}),
        "p_nom_opt": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
        "sub_network": AttributeSpec(dtype=nw.String(), dims={"entity"}),
    }
    return schema(attributes={"network": declared}, results=results)


def network_tables(n) -> dict[str, pd.DataFrame]:
    """`n` as tables keyed by the names `network_schema(n)` declares.

    Every one but the `entity_type` group, which is `network_kinds(n)`: it
    shares its name with the `entity_type` dim, and `from_sources` reads a
    table of that name as the dim's labels as well
    (`test_a_group_named_after_its_into_dim_is_written`).

    Standard types are left out: PyPSA fills them on every network, so they are
    its catalogue rather than this network's components. A stochastic network
    repeats a component per scenario, which collapses to one entity.
    """
    types = _types(n)
    static = {
        c.name: c.static.reset_index().rename(columns={"name": "entity"}) for c in types
    }
    ports = pd.concat([_ports(c) for c in types], ignore_index=True)
    tables = {
        "snapshot": pd.DataFrame({"snapshot": n.snapshots}),
        "entity": network_kinds(n)[["entity"]].assign(deleted=False),
        "connection": ports[["entity", "bus", "role"]],
        "efficiency": pd.concat(
            [
                _per_port(n.c[t], "efficiency", rows)
                for t, rows in ports.groupby("type")
            ],
            ignore_index=True,
        ),
    }
    for attribute in ("p_nom", "carrier"):
        tables[attribute] = (
            pd.concat(
                [
                    static[c.name][["entity", attribute]]
                    for c in types
                    if _has(c, attribute)
                ]
            )
            .drop_duplicates("entity", ignore_index=True)
            .rename(columns={attribute: "value"})
        )
    for attribute in SERIES:
        tables[attribute] = pd.concat(
            [_series(c, attribute) for c in types if _has(c, attribute)],
            ignore_index=True,
        )
    if n.has_scenarios:
        weights = n.scenario_weightings.reset_index()
        tables["scenario"] = weights[["scenario"]]
        tables["weight"] = weights.rename(columns={"weight": "value"})
    return tables


def network_kinds(n) -> pd.DataFrame:
    """The `entity_type` group of `n`: each entity and its component type, once."""
    return pd.concat(
        [
            c.static.reset_index()[["name"]]
            .rename(columns={"name": "entity"})
            .assign(entity_type=c.name)
            for c in _types(n)
        ]
    ).drop_duplicates(ignore_index=True)


def _types(n) -> list:
    """The component types of `n` with members, standard types left out."""
    return [
        c
        for c in n.components
        if not c.static.empty and c.name not in n.standard_type_components
    ]


def _has(c, attribute: str) -> bool:
    """Whether `c` holds `attribute` as PyPSA's registry declares it for the type.

    Not every static column of that name: `ac_dc_meshed` adds a
    `Carrier.marginal_cost`, a different quantity from a generator's.
    """
    return attribute in c.static and attribute in c.defaults.index


def _port_name(stem: str, port: str) -> str:
    """PyPSA's column for `stem` at `port`: `bus0`, `efficiency`, `efficiency2`.

    `bus` is suffixed from `0`, every other per-port column from `2`.
    """
    if stem == "bus":
        return f"bus{port}"
    return stem if port in ("", "1") else f"{stem}{port}"


def _ports(c) -> pd.DataFrame:
    """One type's attachments, `(type, port, entity, bus, role)`, one per bus it names.

    `role` is PyPSA's sign convention written out: a one-port component is
    `attached`, port `0` the `input`, every later port an `output`.
    """
    static = c.static.reset_index().rename(columns={"name": "entity"})
    frames = []
    for port in c.ports:
        column = _port_name("bus", port)
        if column not in static:
            continue
        rows = static[["entity", column]].rename(columns={column: "bus"})
        role = "attached" if port == "" else "input" if port == "0" else "output"
        frames.append(rows[rows["bus"] != ""].assign(type=c.name, port=port, role=role))
    columns = ["type", "port", "entity", "bus", "role"]
    return (
        pd.concat(frames)[columns].drop_duplicates()
        if frames
        else pd.DataFrame(columns=columns)
    )


def _per_port(c, stem: str, ports: pd.DataFrame) -> pd.DataFrame:
    """One type's per-port `stem` as long rows, each carrying the bus of its port."""
    return pd.concat(
        [
            _series(c, _port_name(stem, port)).merge(
                rows[["entity", "bus"]], on="entity"
            )
            for port, rows in ports.groupby("port")
            if _has(c, _port_name(stem, port))
        ]
        or [pd.DataFrame(columns=["entity", "bus", "value"])],
        ignore_index=True,
    )


def _series(c, column: str) -> pd.DataFrame:
    """One type's `column` as long rows: a row per snapshot where it has a series.

    A component with no series holds a constant, a row with `snapshot` NULL;
    `scenario` is a column where the network has one.
    """
    wide = c.dynamic[column] if column in c.dynamic else pd.DataFrame()
    series = (
        wide.rename_axis(index="snapshot", columns=c.static.index.names)
        .melt(ignore_index=False, value_name="value")
        .reset_index()
    )
    constant = c.static.loc[~c.static.index.isin(wide.columns), column]
    rows = pd.concat(
        [series, constant.rename("value").reset_index()], ignore_index=True
    )
    return rows.rename(columns={"name": "entity"})


def export_network(n, revision, con) -> None:
    """Write `n` as `revision`'s layer: `network_tables` through `from_sources`.

    Notes
    -----
    - [sources](https://energy-models.github.io/datarecord/design/sources/)
    """
    write_record(revision.id, from_sources(network_schema(n), network_tables(n)), con)
    write_group(
        layer_dir(revision.id), "entity_type", network_kinds(n).to_dict("records")
    )


def write_schema(schema: Schema, base_uri: str | None = None) -> None:
    """Declare the record's one schema, beside the layers.

    Not per layer: a layer holds only data, so this writes the record-level
    `manifest.json` that every layer in the tree is read under.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    record_write_schema(schema, base_uri)


def write_directory_schema(directory: str, schema: Schema) -> None:
    """Write `manifest.json` *inside* `directory`, for a standalone record.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    Path(directory).mkdir(parents=True, exist_ok=True)
    Path(directory, "manifest.json").write_text(schema.model_dump_json())


def _default_attributes(
    dims: dict[str, nw.dtypes.DType], groups: dict[str, dict[str, str]]
):
    """The attributes tests write, declared over whichever dims are in play.

    Writing an attribute the schema does not declare is rejected, since its
    `dims` are what say which columns its file carries - so every attribute a
    test writes has to be declared, and these are the ones they write.

    Addressed over every declared dim rather than a narrower set, which is the
    widest shape and so the one that accepts any row a test writes.
    `efficiency` is the exception, being over the `connection` group where one
    is declared: that is what puts a `bus` column on its file.

    `weight` is the other, addressed by `scenario` alone - so it is a column of
    `dims/scenario.parquet` rather than a long row, and it is declared because
    `write_scenarios` writes that column and an axis file rejects one no
    declaration accounts for.
    """
    varying = {"entity", *dims}
    connection = "connection" if "connection" in groups else "entity"
    declared = {
        "p_nom": AttributeSpec(dtype=nw.Float64(), dims=varying),
        "e_nom": AttributeSpec(dtype=nw.Float64(), dims=varying),
        "p_max_pu": AttributeSpec(dtype=nw.Float64(), dims=varying),
        "p_min_pu": AttributeSpec(dtype=nw.Float64(), dims=varying),
        "marginal_cost": AttributeSpec(
            dtype=nw.Float64(), dims=varying, breakpoints=True
        ),
        "efficiency": AttributeSpec(dtype=nw.Float64(), dims={connection, *dims}),
    }
    if "scenario" in dims:
        declared["weight"] = AttributeSpec(
            dtype=nw.Float64(),
            dims={"scenario"},
            description="How much this scenario counts in the expectation.",
        )
    return declared


def schema(
    *,
    partial: set[str] = {"scenario"},
    attributes: dict[str, dict[str, AttributeSpec]] | None = None,
    dims: dict[str, nw.dtypes.DType] = {
        "snapshot": nw.Datetime(),
        "period": nw.Int64(),
        "scenario": nw.String(),
    },
    groups: dict[str, dict[str, str]] = {
        "connection": {"entity": "entity", "bus": "bus"}
    },
    within: dict[str, set[str]] | None = None,
    results: dict[str, AttributeSpec] | None = None,
) -> Schema:
    """A schema shaped like the PyPSA records most tests build on.

    The `entity` axis and a `connection` group over `(entity, bus)`, and three
    declared dims. Override `partial` to pin a different layering granularity,
    `dims` to declare another axis, `groups` to declare a different sparse
    relation, `within` to nest one axis inside another, `results` to declare
    what a solve writes back.

    `entity` and every group coordinate are declared dims and are `partial`:
    a layer patches one component's value, or one connection's, without
    restating the rest, which is what `partial` means. The schema requires it,
    so this supplies it rather than leaving each caller to.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    """
    nesting = within or {}
    # Callers declare per type, which is how a modelling framework thinks; the
    # schema stores one spec per attribute, record-wide.
    flat: dict[str, AttributeSpec] = {}
    for attrs in (attributes or {}).values():
        for attr, spec in attrs.items():
            flat.setdefault(attr, spec)
    # Declared whether or not a caller named them: a test writing `p_max_pu`
    # needs it declared, and one passing `attributes=` is narrowing what a type
    # *carries* rather than shortening the record's vocabulary.
    for attr, spec in _default_attributes(dims, groups).items():
        flat.setdefault(attr, spec)
    # A group's coordinates are dims like any other, so they are declared here
    # rather than assumed - which is what lets a caller pass a group over
    # coordinates that are not called `bus`.
    coordinates = {c for over in groups.values() for c in over}
    declared = {
        "entity": nw.String(),
        **{c: nw.String() for c in coordinates},
        **dims,
    }
    return Schema(
        groups={g: Group(over=over) for g, over in groups.items()}
        | {"entity_type": Group(over=["entity"], into="entity_type")},
        dimensions={
            d: Dimension(dtype=t, within=frozenset(nesting.get(d, set())))
            for d, t in declared.items()
        }
        | {"entity_type": Dimension(dtype=nw.String())},
        attributes=flat,
        results=results or {},
        partial=frozenset({"entity", *coordinates, *partial}),
    )


def relation(revision, attribute: str):
    """The resolved long relation for one input attribute, as a DuckDB relation.

    A test helper rather than a `Revision` method: `Revision` presents its data
    through `.record` (a `Record`), and a DuckDB-shaped accessor beside it would
    duplicate `record.attributes[attr]` while inverting what `outputs` means -
    a relation on the revision against a `Frames` mapping on the record. Tests
    want relations because they assert on `.df()`, so the affordance lives here.
    """
    return revision.resolver.attribute(attribute)


def outputs(revision, attribute: str):
    """One result attribute as a DuckDB relation; outputs do not overlay.

    Notes
    -----
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    return revision.resolver.attribute(attribute, "outputs")
