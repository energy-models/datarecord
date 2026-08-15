"""Overlay semantics over a parent/child pair.

Notes
-----
- [layered resolution](https://energy-models.github.io/datarecord/design/layers/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

import pandas as pd
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.layered.write import write_record
from datarecord.record import EMPTY
from datarecord.schema import AttributeSpec
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import export_network, outputs, relation, tombstone, write_input


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


def test_child_overwrite_reaches_model(con, parent):
    """The overwrite turns a series component into a static one in the model."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )

    n = PyPSA.build(child.record)
    assert n.c["Generator"].static.loc["Manchester Wind", "p_max_pu"] == 0.42
    assert "Manchester Wind" not in n.c["Generator"].dynamic["p_max_pu"].columns
    assert "Norway Wind" in n.c["Generator"].dynamic["p_max_pu"].columns


def test_tombstone_removes_component(con, parent):
    """A tombstone removes the component from every attribute and dimension.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    child = parent.child()
    tombstone(layer_dir(child.id), "Generator", ["Norway Gas"])

    om = child.node_cache.components.df()
    assert "Norway Gas" not in set(om["entity"])
    assert "Norway Gas" in set(parent.node_cache.components.df()["entity"])

    n = PyPSA.build(child.record)
    assert "Norway Gas" not in n.c["Generator"].static.index
    assert "Norway Wind" in n.c["Generator"].static.index


def test_child_adds_attribute(con, parent):
    """A child may write an attribute no ancestor had."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_min_pu",
        [{"entity": "Norway Gas", "value": 0.1}],
    )

    n = PyPSA.build(child.record)
    assert n.c["Generator"].static.loc["Norway Gas", "p_min_pu"] == 0.1
    # Untouched generators keep the catalog default.
    assert n.c["Generator"].static.loc["Norway Wind", "p_min_pu"] == 0.0


def test_sibling_branch_unaffected(con, parent):
    """A tombstone only affects the branch that carries it.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    deleting = parent.child()
    tombstone(layer_dir(deleting.id), "Generator", ["Norway Gas"])
    sibling = parent.child()

    assert "Norway Gas" not in set(deleting.node_cache.components.df()["entity"])
    assert "Norway Gas" in set(sibling.node_cache.components.df()["entity"])


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


def test_closed_child_reads_own_node_cache(con, parent):
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
    n = PyPSA.build(reloaded.record)
    assert n.c["Generator"].static.loc["Manchester Wind", "p_max_pu"] == 0.42

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
    amended = read_schema()
    # Declared once, record-wide, then subscribed to by the type that carries
    # it - the two halves an amendment now has.
    amended.attributes["p_min_pu"] = AttributeSpec(
        dtype="DOUBLE", dims={"snapshot"}, default=0.25
    )
    was = amended.component_types["Generator"]
    amended.component_types["Generator"] = was.model_copy(
        update={"attributes": was.attributes | {"p_min_pu"}}
    )
    write_schema(amended)

    # Adding an attribute is compatible, so the layers written before the
    # amendment stay readable (https://energy-models.github.io/datarecord/design/schema/#versioning).
    assert amended.compatible_with(read_schema()) == []

    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_min_pu",
        [{"entity": "Norway Gas", "value": 0.1}],
    )
    n = PyPSA.build(child.record)
    assert n.c["Generator"].static.loc["Norway Gas", "p_min_pu"] == 0.1
    # And the amendment is visible from the record, not just from the layer
    # that happens to carry a row for it.
    assert "p_min_pu" in child.record.schema.attributes_for("Generator")


def test_a_schema_narrowing_is_refused(con, parent, ac_dc):
    """A layer cannot redefine what an attribute means.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    narrowed = read_schema()
    narrowed.attributes["p_max_pu"] = AttributeSpec(dtype="DOUBLE", dims=frozenset())

    class _Narrowed:
        """The record's own source, with one attribute's dims taken away."""

        schema = narrowed
        dims = EMPTY
        components = EMPTY
        connections = EMPTY
        attributes = EMPTY
        outputs = EMPTY

        def flags(self, ctype):
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

    n = PyPSA.build(grandchild.record)
    pd.testing.assert_index_equal(
        n.c["Generator"].static.index,
        ac_dc.c["Generator"].static.index,
        check_names=False,
    )
