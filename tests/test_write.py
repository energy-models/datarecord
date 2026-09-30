# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a layer from long-format frames.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
"""

from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.layered.resolve import read_schema
from datarecord.layered.revision import Record
from datarecord.layered.write import write_record
from datarecord.record import EMPTY, LazyFrames
from datarecord.schema import Schema
from tests.fixtures import export_network, relation, schema


class _Source:
    """A minimal `Record` over ready-made frames, counting each build."""

    def __init__(
        self,
        schema,
        attributes=None,
        relations=None,
        dims=None,
    ):
        self._schema = schema
        self.built: list[str] = []
        self._attributes = attributes or {}
        self._relations = relations or {}
        self._dims = dims or {}

    @property
    def schema(self):
        return self._schema

    def _frames(self, mapping, tag):
        def build(key):
            self.built.append(f"{tag}:{key}")
            return nw.from_native(mapping[key]).lazy()

        return LazyFrames(tuple(mapping), build)

    @property
    def dims(self):
        return self._frames(self._dims, "dims") if self._dims else EMPTY

    @property
    def relations(self):
        return self._frames(self._relations, "relations")

    @property
    def attributes(self):
        return self._frames(self._attributes, "attributes")

    def flags(self, **labels):
        return {}


_SCHEMA = schema()


def _long(**overrides) -> pd.DataFrame:
    """One long row carrying its attribute's own coordinates, and no others.

    Shaped from the spec rather than spelled: a source handing over a column
    the attribute is not addressed by is what `write_record` now rejects, so a
    helper that spelled every declared dim would be testing against a record no
    reader would accept.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    row = {
        "entity": "steel_dri",
        "bus": None,
        "snapshot": None,
        "scenario": None,
        "period": None,
        "attribute": "p_nom",
        "breakpoint": None,
        "value": 1.0,
    }
    row.update(overrides)
    columns = _SCHEMA.long_columns_for(str(row["attribute"]))
    return pd.DataFrame([{c: row.get(c) for c in columns}])


# -- the lazy mapping (https://energy-models.github.io/datarecord/design/format/) -------------------------------------------------


def test_source_is_explorable_without_building(con, base_uri):
    """Keys list, `in` answers, iteration repeats - none of it builds a frame."""
    source = _Source(_SCHEMA, attributes={"p_nom": _long(), "e_nom": _long()})

    assert list(source.attributes) == ["p_nom", "e_nom"]
    assert "p_nom" in source.attributes
    assert "nope" not in source.attributes
    assert len(source.attributes) == 2
    # Re-iterating works, unlike a generator, and still nothing is built.
    assert list(source.attributes) == ["p_nom", "e_nom"]
    assert source.built == []

    source.attributes["p_nom"]
    assert source.built == ["attributes:p_nom"]

    with pytest.raises(KeyError):
        source.attributes["nope"]


def test_write_record_builds_each_key_once(con, base_uri):
    """The writer looks up every key exactly once, and only what it writes."""
    revision = Revision.create(con)
    source = _Source(
        _SCHEMA,
        attributes={"p_nom": _long(), "e_nom": _long(attribute="e_nom")},
        dims={"entity": pd.DataFrame({"entity": ["steel_dri"]})},
        relations={
            "entity_type": pd.DataFrame(
                {"entity": ["steel_dri"], "entity_type": ["Process"]}
            )
        },
    )
    write_record(revision.id, source, con)

    assert sorted(source.built) == [
        "attributes:e_nom",
        "attributes:p_nom",
        "dims:entity",
        "relations:entity_type",
    ], "each key the source lists is built exactly once"


# -- creating a layer -------------------------------------------------------


def test_write_record_creates_a_new_layer(con, base_uri):
    """Files land where `layer_dir` says - data only, no schema.

    The record's one schema goes beside `layers/`, so a layer directory holds
    nothing but data. That is what keeps it a plain parquet directory a reader
    knowing nothing about layering can open.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    revision = Revision.create(con)
    write_record(revision.id, _Source(_SCHEMA, attributes={"p_nom": _long()}), con)

    base = Path(layer_dir(revision.id))
    assert (base / "attributes" / "p_nom.parquet").exists()
    assert not (base / "manifest.json").exists()
    # Written once for the whole tree, and it is what the layer is read under.
    assert read_schema() == _SCHEMA


def test_no_layer_file_carries_order_key(con, base_uri, ac_dc, tmp_path):
    """`order_key` is the fold's answer about a frame, never a column of one.

    A source handing over *resolved* frames carries it - which is what
    committing a `WorkingRecord` to a `Directory` does, the record itself being
    what is written. Writing it would put a struct column in files the
    format promises a foreign reader can open, and would look like stored order
    where the fold always re-derives it from file order.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    from datarecord.mutable import Directory, WorkingRecord

    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    staged = WorkingRecord(revision.record, con)
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    out = str(tmp_path / "flat")
    staged.commit(Directory(out))

    written = sorted(Path(out).rglob("*.parquet"))
    assert written, "the commit must have written something to check"
    for path in written:
        columns = con.sql(f"SELECT * FROM read_parquet('{path}')").columns
        assert "order_key" not in columns, f"{path.relative_to(out)} carries order_key"


def test_a_directory_target_carries_its_own_schema(con, base_uri, tmp_path):
    """A standalone record *is* one record, so its schema goes in the directory.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    out = str(tmp_path / "standalone")
    write_record(None, _Source(_SCHEMA, attributes={"p_nom": _long()}), con, uri=out)

    assert (Path(out) / "manifest.json").exists()
    assert Record.at(out, con).schema == _SCHEMA

    # And it is that file answering, not the connection's root: a standalone
    # record is one whole record, so it must read the same through a connection
    # rooted somewhere with no manifest at all.
    from datarecord import duck

    elsewhere = duck.connect(base_uri=str(tmp_path / "unrelated"))
    try:
        assert read_schema(elsewhere) == Schema(), "the other root declares nothing"
        assert Record.at(out, elsewhere).schema == _SCHEMA
    finally:
        elsewhere.close()


def test_write_record_refuses_an_existing_layer(con, base_uri):
    """A whole-layer write never half-replaces what a record already holds.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """
    revision = Revision.create(con)
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    write_record(revision.id, source, con)

    with pytest.raises(FileExistsError, match="already exists"):
        write_record(revision.id, source, con)


def test_a_file_carries_only_its_own_attributes_coordinates(con, base_uri, ac_dc):
    """One attribute is one file, so one column set - not every declared dim.

    A component attribute has no `port` column, a per-port attribute does, and
    neither carries a dim it is not over. The uniform prefix this replaces put
    an all-NULL `bus` on every file and a `period` column on attributes that
    never vary over one.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    attribute_dir = Path(layer_dir(revision.id), "attributes")

    def columns(attribute: str) -> set[str]:
        return set(
            con.read_parquet(str(attribute_dir / f"{attribute}.parquet")).columns
        )

    assert columns("p_max_pu") == {
        "entity",
        "snapshot",
        "attribute",
        "breakpoint",
        "value",
    }, "a component attribute carries `entity`, not `port`"

    assert columns("efficiency") == {
        "port",
        "snapshot",
        "attribute",
        "breakpoint",
        "value",
    }, "a per-port attribute carries `port`, and no dim it is not over"


# -- validation -------------------------------------------------------------


def test_write_record_rejects_an_undeclared_attribute(con, base_uri):
    """An attribute with no spec has no shape, so there is nothing to write it as.

    Its `dims` are what say which columns the file carries, so writing one the
    schema does not declare would put a file in `attributes/` whose column set no
    reader could derive.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    revision = Revision.create(con)
    source = _Source(_SCHEMA, attributes={"not_declared": _long(attribute="nope")})

    with pytest.raises(ValueError, match="not a declared attribute"):
        write_record(revision.id, source, con)


def test_write_record_rejects_a_coordinate_the_attribute_lacks(con, base_uri):
    """A column the attribute is not addressed by is a disagreement, not a spare.

    The read path projects an attribute's own coordinates, so a `bus` on a
    component attribute would be written and never read - and a source emitting
    one means something different by the attribute than the schema does.
    Reported rather than dropped, since silently narrowing would hide that.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    revision = Revision.create(con)
    wide = _long().assign(bus=None)
    source = _Source(_SCHEMA, attributes={"p_nom": wide})

    with pytest.raises(
        ValueError, match=r"carries columns \['bus'\].*not addressed by"
    ):
        write_record(revision.id, source, con)


def test_write_record_rejects_a_missing_long_column(con, base_uri):
    """A frame the fold could not resolve is refused before anything is written."""
    revision = Revision.create(con)
    short = _long().drop(columns=["breakpoint"])
    source = _Source(_SCHEMA, attributes={"p_nom": short})

    with pytest.raises(ValueError, match="missing long-schema columns.*breakpoint"):
        write_record(revision.id, source, con)
    assert not Path(layer_dir(revision.id)).exists()


def test_write_record_rejects_a_relation_frame_missing_a_column(con, base_uri):
    """A relation's row is keyed by its columns, so one lacking them misresolves.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
    """
    revision = Revision.create(con)
    source = _Source(
        _SCHEMA,
        relations={"connection": pd.DataFrame({"entity": ["steel_dri"]})},
    )

    with pytest.raises(ValueError, match="columns.*bus"):
        write_record(revision.id, source, con)


def test_write_record_rejects_a_nested_axis_without_its_parent(con, base_uri):
    """A `within` dim's file needs a column per parent, or the fold miskeys it.

    `snapshot within period` makes the axis key `(period, snapshot)`, so a
    `snapshot.parquet` carrying only timestamps would fold two periods'
    identically labelled hours into one row.

    Notes
    -----
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    """
    revision = Revision.create(con)
    nested = schema(within={"snapshot": {"period"}})
    source = _Source(
        nested,
        dims={"snapshot": pd.DataFrame({"snapshot": pd.to_datetime(["2020-01-01"])})},
    )

    with pytest.raises(ValueError, match="axis key columns.*period"):
        write_record(revision.id, source, con)
    assert not Path(layer_dir(revision.id)).exists()


@pytest.mark.parametrize(
    ("dim", "column"),
    [
        pytest.param("scenario", "nonsense", id="undeclared"),
        pytest.param("entity", "p_max_pu", id="declared-over-more-than-the-axis"),
    ],
)
def test_write_record_rejects_an_undeclared_axis_column(con, base_uri, dim, column):
    """An axis file carries only the attributes addressed by that axis alone.

    A column no declaration accounts for would be read back with no dtype and
    no meaning. A column the schema declares over more dims, as `p_max_pu` is
    over `(entity, snapshot)`, belongs in `attributes/` as long rows: on the entity
    axis it would shadow nothing and be read by nothing. Both are refused
    rather than carried along.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    revision = Revision.create(con)
    source = _Source(
        schema(),
        dims={dim: pd.DataFrame({dim: ["x"], column: [1.0]})},
    )

    with pytest.raises(ValueError, match=f"does not declare for the '{dim}' axis"):
        write_record(revision.id, source, con)
    assert not Path(layer_dir(revision.id)).exists()


def test_an_axis_carries_the_attributes_addressed_by_it_alone(con, base_uri):
    """`weight` over `scenario` alone is a column of that axis's file.

    Declared, so it round-trips with a dtype - which is what distinguishes it
    from the undeclared column above.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    revision = Revision.create(con)
    declared = schema()
    assert "weight" in declared.attributes_on("scenario")

    source = _Source(
        declared,
        dims={"scenario": pd.DataFrame({"scenario": ["high"], "weight": [0.4]})},
    )
    write_record(revision.id, source, con)

    axis = revision.resolver.dims.axes["scenario"].df()
    assert dict(zip(axis["scenario"], axis["weight"])) == {"high": 0.4}


def test_written_layer_overlays(con, base_uri, ac_dc):
    """A written layer is an ordinary layer: a child patches it as any other."""
    from tests.fixtures import write_attribute

    root = Revision.create(con)
    export_network(ac_dc, root, con)
    root.materialise()

    child = root.child()
    write_attribute(
        layer_dir(child.id),
        "p_nom",
        [{"entity": "Manchester Wind", "value": 999.0}],
    )

    resolved = relation(child, "p_nom").filter("entity = 'Manchester Wind'").df()
    assert list(resolved["value"]) == [999.0]
