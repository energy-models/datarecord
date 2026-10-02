# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Overlay semantics over a parent/child pair.

Notes
-----
- [layered resolution](https://energy-models.github.io/datarecord/design/layers/)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.layered.resolve import (
    Resolver,
    read_schema,
    sources_to_read,
    write_schema,
)
from datarecord.layered.revision import ancestry
from datarecord.layered.sources import ParquetLayer
from datarecord.layered.write import write_record
from datarecord.record import EMPTY
from datarecord.schema import AttributeSpec
from datarecord.sources import to_sources
from tests.fixtures import (
    export_network,
    names,
    outputs,
    relation,
    tombstone,
    write_input,
)


@pytest.fixture
def parent(con, base_uri, ac_dc):
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    revision.materialise()
    return revision


def test_child_overwrites_component(con, parent):
    """A child's rows replace all of that component's rows for the attribute.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )

    df = relation(child, "p_max_pu").df()
    manchester = df[df["entity"] == "Manchester Wind"]
    # The parent's 10 series rows are gone, replaced by the child's single row.
    assert len(manchester) == 1
    assert manchester["value"].iloc[0] == 0.42
    assert pd.isna(manchester["snapshot"].iloc[0])

    # Siblings on the same layer are untouched.
    assert len(df[df["entity"] == "Norway Wind"]) == 10


def test_child_overwrite_reaches_a_consumer(con, parent, ac_dc):
    """The overwrite turns a series component into a constant one for a consumer.

    `to_sources` expands the child's one NULL-snapshot row to every snapshot,
    so the constant is what a consumer reads at each, while a sibling keeps its
    own series.
    """
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )

    rows = (
        to_sources(child.record, names=["p_max_pu"])["p_max_pu"]
        .collect()
        .to_native()
        .to_pandas()
    )
    manchester = rows[rows["entity"] == "Manchester Wind"]
    assert len(manchester) == len(ac_dc.snapshots), "one row per snapshot"
    assert set(manchester["value"]) == {0.42}, "the constant at every snapshot"
    norway = rows[rows["entity"] == "Norway Wind"].sort_values("snapshot")
    assert (
        norway["value"].tolist()
        == ac_dc.c["Generator"].dynamic["p_max_pu"]["Norway Wind"].tolist()
    ), "the sibling keeps its series"


def test_tombstone_removes_component(con, parent):
    """A tombstone removes the component from every attribute and dimension.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    child = parent.child()
    tombstone(layer_dir(child.id), "Generator", ["Norway Gas"])

    om = child.resolver.dims.axes["entity"].df()
    assert "Norway Gas" not in set(om["entity"])
    assert "Norway Gas" in set(parent.resolver.dims.axes["entity"].df()["entity"])

    generators = names(child.record, "Generator")
    assert "Norway Gas" not in generators
    assert "Norway Wind" in generators


def test_child_adds_attribute(con, parent):
    """A child may write an attribute no ancestor had."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_min_pu",
        [{"entity": "Norway Gas", "value": 0.1}],
    )

    rows = child.record.attributes["p_min_pu"].collect().to_native().to_pandas()
    values = rows.groupby("entity")["value"].apply(list).to_dict()
    assert values["Norway Gas"] == [0.1]
    assert values["Norway Wind"] == [0.0], "an untouched generator keeps its value"


def test_sibling_branch_unaffected(con, parent):
    """A tombstone only affects the branch that carries it.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    deleting = parent.child()
    tombstone(layer_dir(deleting.id), "Generator", ["Norway Gas"])
    sibling = parent.child()

    assert "Norway Gas" not in set(deleting.resolver.dims.axes["entity"].df()["entity"])
    assert "Norway Gas" in set(sibling.resolver.dims.axes["entity"].df()["entity"])


def test_grandchild_resolves_through_ancestry(con, parent):
    """Resolution walks the whole root->node path, nearest layer winning."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )
    child.materialise()

    grandchild = child.child()
    write_input(
        layer_dir(grandchild.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.99}],
    )

    df = relation(grandchild, "p_max_pu").df()
    manchester = df[df["entity"] == "Manchester Wind"]
    assert len(manchester) == 1
    assert manchester["value"].iloc[0] == 0.99
    assert len(df[df["entity"] == "Norway Wind"]) == 10


def test_closed_child_reads_own_resolver(con, parent):
    """Reading a closed non-root record uses its own persisted dims/manifest/map.

    Its `ancestry_since_closed` is just itself, so the raw layer (which has
    no `dims/` or `manifest.json` of its own here) cannot be the source.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )
    child.materialise()

    reloaded = Revision.get(child.id, con)
    rows = reloaded.record.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert rows[rows["entity"] == "Manchester Wind"]["value"].tolist() == [0.42]

    df = relation(reloaded, "p_max_pu").df()
    assert df[df["entity"] == "Manchester Wind"]["value"].tolist() == [0.42]


def test_outputs_do_not_overlay(con, parent):
    """Results come from the node's own layer only.

    Notes
    -----
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    child = parent.child()
    assert outputs(child, "p").df().empty


def test_resolved_reads_same_as_unresolved(con, parent):
    """A materialised node reads identically to the same node folded from its layers.

    The invariant the base/source split protects: a materialised ancestor's
    resolved fold, read as the base, gives every reader the same answer as
    re-folding the whole ancestry from the root. Materialise a grandchild, then
    build one node cache the truncated way (base = the materialised parent) and
    one the long way (every layer as its own `ParquetLayer`, no truncation), and
    assert they agree on the owner map, the entity axis, every attribute
    relation, and the group frames.
    """
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )
    child.materialise()

    grandchild = child.child()
    write_input(
        layer_dir(grandchild.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.99}],
    )

    # `unresolved` folds the whole ancestry from the root: every layer read from
    # its own directory, no base short-circuit, even though the ancestors are
    # materialised. `_Unmaterialised` forces `materialised()` to `None` so the
    # fold cannot take a base and must re-derive what the base would carry.
    class _Unmaterialised(ParquetLayer):
        def materialised(self, con, schema):  # noqa: ARG002
            return None

    schema = read_schema(con)
    full = ancestry(con, grandchild.id)
    truncated = Resolver(grandchild.id, sources_to_read(full, con, schema), con, schema)
    unresolved = Resolver(
        grandchild.id, [_Unmaterialised(uid, schema, con) for uid in full], con, schema
    )

    def ownership(nc):
        return sorted(
            nc.inputs.project("attribute, entity, layer_uuid").fetchall(),
            key=str,
        )

    assert ownership(truncated) == ownership(unresolved), (
        "the owner map folded through the base matches folding from the root"
    )

    t_axis, u_axis = truncated.dims.axes["entity"], unresolved.dims.axes["entity"]
    assert t_axis is not None and u_axis is not None
    assert set(t_axis.df()["entity"]) == set(u_axis.df()["entity"]), (
        "the resolved entity axis is the same either way"
    )

    for attr in truncated.attributes():
        a = sorted(truncated.attribute(attr).fetchall(), key=str)
        b = sorted(unresolved.attribute(attr).fetchall(), key=str)
        assert a == b, f"{attr} resolves the same through the base as from the root"

    manchester = truncated.attribute("p_max_pu").df()
    assert manchester[manchester["entity"] == "Manchester Wind"]["value"].tolist() == [
        0.99
    ], "grandchild's value wins over the materialised base"


def test_a_new_attribute_is_a_schema_amendment(con, parent):
    """Adding an attribute amends the record's one schema, not a layer's.

    A schema is not layered data: folding it would let a layer redefine what
    an attribute means, and make the schema unknowable without walking the
    ancestry. So the amendment lands beside the layers, and every layer in the
    tree - including ones already written - is read under it.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    before = read_schema()
    assert "availability" not in before.attributes, "the attribute must be new"
    amended = read_schema()
    amended.attributes["availability"] = AttributeSpec(
        dtype=nw.Float64(), dims={"entity", "snapshot"}, default=0.25
    )
    assert amended.compatible_with(before) == [], (
        "adding an attribute leaves the layers written before it readable"
    )
    write_schema(amended)

    child = parent.child()
    write_input(
        layer_dir(child.id),
        "availability",
        [{"entity": "Norway Gas", "value": 0.1}],
    )
    rows = child.record.attributes["availability"].collect().to_pandas()
    assert rows.set_index("entity")["value"].to_dict() == {"Norway Gas": 0.1}, (
        "a layer written after the amendment carries the new attribute"
    )
    assert "availability" in parent.record.schema.attributes, (
        "a layer written before the amendment is read under the amended schema"
    )


def test_a_schema_narrowing_is_refused(con, parent, ac_dc):
    """A layer cannot redefine what an attribute means.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    narrowed = read_schema()
    narrowed.attributes["p_max_pu"] = AttributeSpec(
        dtype=nw.Float64(), dims=frozenset()
    )

    class _Narrowed:
        """The record's own source, with one attribute's dims taken away."""

        schema = narrowed
        dims = EMPTY
        groups: dict = {}
        attributes = EMPTY
        outputs = EMPTY

        def flags(self, **labels):
            return {}

    child = parent.child()
    with pytest.raises(ValueError, match="no longer varies over"):
        write_record(child.id, _Narrowed(), con)


def test_member_order_survives_closed_intermediate(con, parent, ac_dc):
    """Component order still follows the true owning layer through a closed
    intermediate node that changes nothing about membership.

    `members()` resolves straight from the owner map's `order_key`, not from
    a per-file depth lookup over `ancestry_since_closed` - which wouldn't
    even find `parent` here, since `middle` sits between it and `grandchild`.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    middle = parent.child()
    middle.materialise()
    grandchild = middle.child()

    assert names(grandchild.record, "Generator") == list(
        ac_dc.c["Generator"].static.index
    ), "the root's member order"
