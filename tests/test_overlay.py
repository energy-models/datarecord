# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Overlay semantics over a parent/child pair.

Notes
-----
- [layered resolution](https://energy-models.github.io/datarecord/design/layers/tree/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from pathlib import Path

import pandas as pd
import pytest

from datarecord.protocols import EMPTY
from datarecord.resolve.record import Record
from datarecord.resolve.sources import ParquetLayer
from datarecord.schema.specs import AttributeSpec
from datarecord.store.cache import sources_to_read
from datarecord.store.store import lineage
from tests.fixtures import (
    empty_child,
    empty_root,
    relation,
    schema,
    write_attribute,
    write_component,
    write_multi_dim,
)


@pytest.fixture
def parent(store):
    """A materialised root: two generators with a three-snapshot `p_max_pu`.

    `Norway Wind` also carries an `efficiency` on its connection, so a
    attribute keyed by a multi-dim folds through the base too.
    """
    layer = empty_root(store, schema())
    names = ["Manchester Wind", "Norway Wind"]
    write_component(layer, "Generator", [{"entity": n} for n in names])
    write_multi_dim(
        layer, "connection", [{"connection": {"entity": "Norway Wind", "bus": "NO"}}]
    )
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": n, "snapshot": f"2030-01-0{d}", "value": 0.1 * d}
            for n in names
            for d in (1, 2, 3)
        ],
    )
    write_attribute(
        layer, "efficiency", [{"entity": "Norway Wind", "bus": "NO", "value": 0.9}]
    )
    revision = layer.commit()
    revision.materialise()
    return revision


def test_child_overwrites_component(parent):
    """A child's rows replace all of that component's rows for the attribute.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    """
    child = empty_child(parent)
    write_attribute(child, "p_max_pu", [{"entity": "Manchester Wind", "value": 0.42}])

    df = relation(child.commit(), "p_max_pu").df()
    manchester = df[df["entity"] == "Manchester Wind"]
    # The parent's 3 series rows are gone, replaced by the child's single row.
    assert len(manchester) == 1
    assert manchester["value"].iloc[0] == 0.42
    assert pd.isna(manchester["snapshot"].iloc[0])

    # Siblings on the same layer are untouched.
    assert len(df[df["entity"] == "Norway Wind"]) == 3


def test_closed_child_reads_own_cache(store, parent):
    """Reading a closed non-root record uses its own persisted dims/manifest/map.

    Its `ancestry_since_closed` is just itself, so the raw layer (which has
    no `dims/` or `manifest.json` of its own here) cannot be the source.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    child = empty_child(parent)
    write_attribute(child, "p_max_pu", [{"entity": "Manchester Wind", "value": 0.42}])
    child.commit().materialise()

    reloaded = store.revision(child.id)
    assert set(reloaded.record.dims["entity"].df()["entity"]) == {
        "Manchester Wind",
        "Norway Wind",
    }, "the entity axis comes from the root through the cache"
    df = relation(reloaded, "p_max_pu").df()
    assert df[df["entity"] == "Manchester Wind"]["value"].tolist() == [0.42]
    assert len(df[df["entity"] == "Norway Wind"]) == 3


def test_resolved_reads_same_as_unresolved(store, parent):
    """A materialised node reads identically to the same node folded from its layers.

    The invariant the base/source split protects: a materialised ancestor's
    resolved fold, read as the base, gives every reader the same answer as
    re-folding the whole ancestry from the root. Materialise a grandchild, then
    build one node cache the truncated way (base = the materialised parent) and
    one the long way (every layer as its own `ParquetLayer`, no truncation), and
    assert they agree on the owner map, the entity axis, every attribute
    relation, and the multi-dim axes.
    """
    child_layer = empty_child(parent)
    write_attribute(
        child_layer, "p_max_pu", [{"entity": "Manchester Wind", "value": 0.42}]
    )
    child = child_layer.commit()
    child.materialise()

    grandchild = empty_child(child)
    write_attribute(
        grandchild, "p_max_pu", [{"entity": "Manchester Wind", "value": 0.99}]
    )
    grandchild.commit()

    # `unresolved` folds the whole ancestry from the root: every layer read from
    # its own directory, no base short-circuit, even though the ancestors are
    # materialised. `_Unmaterialised` forces `materialised()` to `None` so the
    # fold cannot take a base and must re-derive what the base would carry.
    class _Unmaterialised(ParquetLayer):
        def materialised(self):
            return None

    full = lineage(store, grandchild.id)
    workspace = store.workspace
    truncated = Record(grandchild.id, sources_to_read(full, store), workspace)
    unresolved = Record(
        grandchild.id,
        [_Unmaterialised(manifest, store) for manifest in full],
        workspace,
    )

    def ownership(record):
        return sorted(
            record._fold.owner_map.project("attribute, entity, layer_uuid").fetchall(),
            key=str,
        )

    assert ownership(truncated) == ownership(unresolved), (
        "the owner map folded through the base matches folding from the root"
    )

    assert set(truncated.dims["entity"].df()["entity"]) == set(
        unresolved.dims["entity"].df()["entity"]
    ), "the resolved entity axis is the same either way"

    for attr in truncated.attributes:
        a = sorted(truncated.attributes[attr].fetchall(), key=str)
        b = sorted(unresolved.attributes[attr].fetchall(), key=str)
        assert a == b, f"{attr} resolves the same through the base as from the root"

    manchester = truncated.attributes["p_max_pu"].df()
    assert manchester[manchester["entity"] == "Manchester Wind"]["value"].tolist() == [
        0.99
    ], "grandchild's value wins over the materialised base"
    assert len(manchester[manchester["entity"] == "Norway Wind"]) == 3, (
        "an untouched sibling keeps the root's series through the ancestry"
    )


def test_a_child_widens_the_schema_with_a_new_attribute(parent):
    """A child declares an attribute its parent's schema lacks, in its own manifest.

    The child's manifest holds the full schema, so the record is read under it,
    and the parent's layers stay as they were written.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    amended = parent.manifest.schema.model_copy(deep=True)
    amended.attributes["site_quality"] = AttributeSpec(dtype="float", dims={"entity"})
    assert amended.compatible_with(parent.manifest.schema) == []

    child_layer = empty_child(parent, amended)
    write_attribute(
        child_layer, "site_quality", [{"entity": "Norway Wind", "value": 0.1}]
    )
    child = child_layer.commit()
    assert relation(child, "site_quality").df()["value"].tolist() == [0.1]
    assert "site_quality" in child.record.schema.attributes
    assert "site_quality" not in parent.record.schema.attributes, (
        "the parent reads as written"
    )


def test_a_schema_narrowing_is_refused(store, parent):
    """A layer cannot redefine what an attribute means.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    narrowed = parent.manifest.schema.model_copy(deep=True)
    narrowed.attributes["p_max_pu"] = AttributeSpec(dtype="float", dims=frozenset())

    class _Narrowed:
        """The record's own source, with one attribute's dims taken away."""

        schema = narrowed
        dims = EMPTY
        attributes = EMPTY

        def flags(self):
            return {}

    claim = store.claim(parent=parent)
    with pytest.raises(ValueError, match="no longer varies over"):
        claim.commit(_Narrowed())
    assert [r.id for r in store.revisions()] == [parent.id], (
        "the layer is claimed, but without its manifest nothing reaches it"
    )
    assert not Path(store.manifest_uri(claim.id)).exists()
