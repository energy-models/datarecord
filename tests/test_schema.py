# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The typed schema: declarations, derived keys, validation, versioning.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
"""

import re
from typing import Any

import duckdb
import narwhals as nw
import pytest
from pydantic import ValidationError

from datarecord.duck import DuckTypes
from datarecord.schema import (
    AttributeSpec,
    Dimension,
    Group,
    Schema,
    flag_type,
)


def _schema(**overrides) -> Schema:
    """A schema shaped like a stochastic multi-period record."""
    kwargs: dict[str, Any] = {
        "dimensions": {
            "entity": Dimension(dtype=nw.String()),
            "bus": Dimension(dtype=nw.String()),
            "period": Dimension(dtype=nw.Int64()),
            "timestep": Dimension(dtype=nw.Datetime(), within={"period"}),
            "scenario": Dimension(dtype=nw.String()),
            "entity_type": Dimension(dtype=nw.Enum(["Generator", "Link"])),
        },
        "groups": {
            "connection": Group(over={"entity": "entity", "bus": "bus"}),
            "entity_type": Group(over=["entity"], into="entity_type"),
        },
        # Declared once, record-wide.
        "attributes": {
            "p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
            "p_max_pu": AttributeSpec(
                dtype=nw.Float64(), dims={"entity", "scenario", "timestep"}
            ),
            "marginal_cost": AttributeSpec(
                dtype=nw.Float64(), dims={"entity", "scenario"}, breakpoints=True
            ),
            "carrier": AttributeSpec(dtype=nw.String(), dims={"entity"}),
            # A connection attribute says so by naming the group among its
            # dims, rather than by a field of its own.
            "efficiency": AttributeSpec(
                dtype=nw.Float64(), dims={"connection", "scenario", "timestep"}
            ),
        },
        "partial": frozenset({"entity", "bus", "scenario"}),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- derived keys (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override) -----------------------------------------------------


def test_ownership_is_derived_not_declared():
    """`owned_per` is `dims` and `partial` together, never a third declaration."""
    s = _schema()
    # Varies over both time axes, but only `scenario` is partial among them -
    # so a patch to one timestep restates that scenario's whole series.
    assert s.owned_per("p_max_pu") == frozenset({"entity", "scenario"})
    assert s.owned_per("marginal_cost") == frozenset({"entity", "scenario"})
    # A first-stage decision: one value per component, owned once across
    # every axis it does not vary over.
    assert s.owned_per("p_nom") == frozenset({"entity"})
    assert s.owned_per("carrier") == frozenset({"entity"})


def test_a_scenario_varying_capacity_is_a_schema_violation():
    """`dims` is what forbids it: a capacity is decided before the scenario is known."""
    s = _schema()
    assert "scenario" not in s.attributes["p_nom"].dims
    # Nothing owns it per scenario, so the fold writes NULL there and one value
    # applies to every scenario.
    assert s.owned_per("p_nom") == frozenset({"entity"}), (
        "owned per entity, not scenario"
    )


def test_partial_dims_is_the_union_over_attributes():
    """The fold's key is one fixed tuple, so an unowned dim is NULL rather than absent."""
    s = _schema()
    assert s.partial_dims == ("entity", "bus", "scenario"), (
        "the declared order, not the order `partial` names them"
    )

    wider = _schema(partial=frozenset({"entity", "bus", "scenario", "timestep"}))
    assert wider.partial_dims == ("entity", "bus", "timestep", "scenario"), (
        "a dim made `partial` joins the key"
    )


def test_file_split_follows_dims():
    """Varying over nothing is what puts an attribute in `dims/entity_type/`.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    s = _schema()
    assert not s.attributes["p_nom"].varying
    assert not s.attributes["carrier"].varying
    assert s.attributes["p_max_pu"].varying


# -- group keys (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override) --------------------------------------------------


def test_the_fold_key_is_exactly_partial():
    """No dim joins the fold key by its name: `entity` is a dim like any other."""
    s = Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "scenario": Dimension(dtype=nw.String()),
        },
        partial=frozenset({"scenario"}),
    )
    assert s.partial_dims == ("scenario",), "`entity` is not added to `partial`"


@pytest.mark.parametrize(
    ("groups", "partial", "missing"),
    [
        pytest.param(
            {"connection": Group(over=["entity", "bus"])},
            {"entity"},
            "['bus']",
            id="one-coordinate-of-a-tuple-set",
        ),
        pytest.param(
            {"entity_type": Group(over=["entity"], into="entity_type")},
            set(),
            "['entity']",
            id="the-key-of-a-functional-group",
        ),
        pytest.param(
            {"connection": Group(over=["entity", "bus"])},
            None,
            "['bus', 'entity']",
            id="no-partial-at-all",
        ),
    ],
)
def test_a_group_key_missing_from_partial_is_refused(groups, partial, missing):
    """A layer adds or removes one row of a group, so its key must be `partial`.

    The `into` dim is no key, so `entity_type` need not be named.
    """
    with pytest.raises(
        ValidationError, match=rf"`partial` must name {re.escape(missing)}"
    ):
        Schema(
            dimensions={
                "entity": Dimension(dtype=nw.String()),
                "bus": Dimension(dtype=nw.String()),
                "entity_type": Dimension(dtype=nw.String()),
            },
            groups=groups,
            partial=None if partial is None else frozenset(partial),
        )


def test_an_attribute_broadcasts_over_the_dims_it_names():
    """A coordinate reached through a group is the group's rows, not an axis."""
    s = _schema()
    assert s.broadcast_dims == s.dims, "any declared dim may broadcast"
    assert s.broadcasts_over("p_max_pu") == ("entity", "timestep", "scenario"), (
        "the dims its spec names, in declaration order"
    )
    assert s.broadcasts_over("efficiency") == ("timestep", "scenario"), (
        "`entity` and `bus` come through `connection`, so they do not broadcast"
    )
    assert s.broadcasts_over("undeclared") == (), "a result broadcasts over nothing"


# -- entity types ----------------------------------------------------------


def test_a_functional_group_may_not_key_an_attribute_with_what_it_maps_from():
    """`into` says the label follows from the key, so the row is keyed twice.

    Stated for the entity-type axis, which is the case the format names, but
    the rule is general - see the `country`-over-`bus` case below.
    """
    with pytest.raises(ValidationError, match="keys a row twice over"):
        Schema(
            dimensions={
                "entity": Dimension(dtype=nw.String()),
                "entity_type": Dimension(dtype=nw.Enum(["Bus"])),
            },
            groups={"entity_type": Group(over=["entity"], into="entity_type")},
            attributes={
                "p_nom": AttributeSpec(
                    dtype=nw.Float64(), dims={"entity", "entity_type"}
                )
            },
            partial=frozenset({"entity"}),
        )


def test_the_redundant_addressing_rule_covers_every_functional_group():
    """Not an `entity_type` special case: `country` over `bus` is the same shape."""
    with pytest.raises(ValidationError, match="keys a row twice over"):
        Schema(
            dimensions={
                "bus": Dimension(dtype=nw.String()),
                "country": Dimension(dtype=nw.String()),
            },
            groups={"in_country": Group(over=["bus"], into="country")},
            attributes={
                "x": AttributeSpec(dtype=nw.Float64(), dims={"bus", "country"})
            },
            partial=frozenset({"bus"}),
        )


def test_an_attribute_may_be_addressed_by_the_entity_type_alone():
    """A per-type icon is a value per type, keyed once - an axis-file column.

    The type axis is a dim like any other; what may not key a row alongside it
    is the `entity` the group maps into it.
    """
    s = Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "entity_type": Dimension(dtype=nw.Enum(["Bus", "Generator"])),
        },
        groups={"entity_type": Group(over=["entity"], into="entity_type")},
        attributes={
            "p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
            "icon": AttributeSpec(dtype=nw.String(), dims={"entity_type"}),
        },
        partial=frozenset({"entity"}),
    )
    assert s.attributes_on("entity_type") == ("icon",), "a column of the type axis"
    assert not s.attributes["icon"].varying, "addressed by one dim, so not varying"


def test_several_groups_may_map_entity_into_other_dims():
    """A type relation is a relation like any other, so a record may declare several.

    Refused while a group over `entity` alone made its `into` the one entity-type
    axis: a component's bus could not also be a functional group.
    """
    s = Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "entity_type": Dimension(dtype=nw.String()),
            "bus": Dimension(dtype=nw.String()),
        },
        groups={
            "type_of": Group(over=["entity"], into="entity_type"),
            "bus_of": Group(over=["entity"], into="bus"),
        },
        attributes={"p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"})},
        partial=frozenset({"entity"}),
    )
    assert s.attributes_on("entity") == ("p_nom",), (
        "a constant is an entity-axis column"
    )


# -- nesting (https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis) ----------------------------------------------------------


def test_axis_key_is_parents_then_dim():
    """A nested axis's labels identify only within its parents."""
    s = _schema()
    assert s.axis_key("timestep") == ("period", "timestep")
    assert s.axis_key("period") == ("period",)
    assert s.axis_key("scenario") == ("scenario",)


def test_nesting_is_transitive():
    """Naming a parent pulls in that parent's own parents.

    Notes
    -----
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    """
    s = Schema(
        dimensions={
            "horizon": Dimension(dtype=nw.Int64()),
            "period": Dimension(dtype=nw.Int64(), within={"horizon"}),
            "timestep": Dimension(dtype=nw.Datetime(), within={"period"}),
        }
    )
    assert s.axis_key("timestep") == ("horizon", "period", "timestep")


def test_several_direct_parents():
    """A set, since two axes may each qualify a label without containing each other."""
    s = Schema(
        dimensions={
            "period": Dimension(dtype=nw.Int64()),
            "stage": Dimension(dtype=nw.String()),
            "timestep": Dimension(dtype=nw.Datetime(), within={"period", "stage"}),
        }
    )
    assert s.axis_key("timestep") == ("period", "stage", "timestep")


def test_nesting_must_name_declared_dims():
    with pytest.raises(ValidationError, match="undeclared"):
        Schema(dimensions={"timestep": Dimension(dtype=nw.Datetime(), within={"nope"})})


def test_nesting_must_be_acyclic():
    with pytest.raises(ValidationError, match="cyclic"):
        Schema(
            dimensions={
                "a": Dimension(dtype=nw.Int64(), within={"b"}),
                "b": Dimension(dtype=nw.Int64(), within={"a"}),
            }
        )


def test_a_dim_cannot_be_within_itself():
    with pytest.raises(ValidationError, match="within` itself"):
        Schema(dimensions={"a": Dimension(dtype=nw.Int64(), within={"a"})})


def test_an_attribute_cannot_vary_over_an_undeclared_dim():
    with pytest.raises(ValidationError, match="undeclared"):
        Schema(
            dimensions={"scenario": Dimension(dtype=nw.String())},
            attributes={"p": AttributeSpec(dtype=nw.Float64(), dims={"nope"})},
        )


# -- defaults through the manifest (https://energy-models.github.io/datarecord/design/schema/#attributespec, https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ------------------------------


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), 0.0, None, "AC"])
def test_a_default_survives_the_manifest_round_trip(value):
    """An `inf` default must read back as `inf`, not as "no default".

    JSON has no literal for a non-finite float, and pydantic's own serialiser
    emits `null` for one - which would silently turn an unbounded capacity into
    an absent bound. PyPSA declares `inf` defaults (`p_nom_max`), so this is the
    ordinary case rather than an edge one, and it is only the encoding in
    `AttributeSpec` that keeps it.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    schema = Schema(
        dimensions={"scenario": Dimension(dtype=nw.String())},
        attributes={"p_nom_max": AttributeSpec(dtype=nw.Float64(), default=value)},
    )
    back = Schema.model_validate_json(schema.model_dump_json())
    assert repr(back.attributes["p_nom_max"].default) == repr(value)


# -- versioning (https://energy-models.github.io/datarecord/design/schema/#versioning) -------------------------------------------------------


def test_adding_an_attribute_is_compatible():
    old = _schema()
    new = _schema()
    new.attributes["p_min_pu"] = AttributeSpec(dtype=nw.Float64(), dims={"scenario"})
    assert new.compatible_with(old) == []


def test_widening_dims_is_compatible():
    """Rows that set fewer dims still decode: an unset dim is NULL, and NULL means all."""
    old = _schema()
    new = _schema()
    new.attributes["marginal_cost"] = AttributeSpec(
        dtype=nw.Float64(), dims={"entity", "scenario", "timestep"}, breakpoints=True
    )
    assert new.compatible_with(old) == []


def test_widening_partial_is_compatible():
    """Ownership becomes finer; an old row is owned at the coarser granularity."""
    old = _schema()
    new = _schema(partial=frozenset({"entity", "bus", "scenario", "timestep"}))
    assert new.compatible_with(old) == []


def test_narrowing_dims_is_incompatible():
    old = _schema()
    new = _schema()
    # `entity` kept, so `timestep` is the one axis under test.
    new.attributes["p_max_pu"] = AttributeSpec(
        dtype=nw.Float64(), dims={"entity", "scenario"}
    )
    (reason,) = new.compatible_with(old)
    assert "no longer varies over ['timestep']" in reason


def test_changing_a_dtype_is_incompatible():
    old = _schema()
    new = _schema()
    new.attributes["p_nom"] = AttributeSpec(dtype=nw.Int64(), dims={"entity"})
    (reason,) = new.compatible_with(old)
    assert "Float64 -> Int64" in reason


def test_removing_from_partial_is_incompatible():
    """A layer that patched one value is now a partial override of a whole axis."""
    old = _schema(partial=frozenset({"entity", "bus", "scenario", "timestep"}))
    new = _schema()
    reasons = new.compatible_with(old)
    assert any("no longer `partial`" in r for r in reasons)


def test_changing_nesting_is_incompatible():
    """Un-nesting `timestep` changes the axis key's shape, so old rows misread.

    `new` is `_schema()` with the one difference under test: `timestep` no
    longer `within` `period`, where every other dim is restated unchanged.
    """
    old = _schema()
    new = _schema(
        dimensions={
            **old.dimensions,
            "timestep": Dimension(dtype=nw.Datetime()),
        }
    )
    reasons = new.compatible_with(old)
    assert any("nesting changed" in r for r in reasons)


# -- unit and description (https://energy-models.github.io/datarecord/design/schema/#unit-and-description) ---------------------------------------------


def test_unit_and_description_are_declared_on_both():
    """An axis may carry them too, not just an attribute.

    Notes
    -----
    - [unit and description](https://energy-models.github.io/datarecord/design/schema/#unit-and-description)
    """
    s = Schema(
        dimensions={
            "vintage": Dimension(
                dtype=nw.Int64(), unit="year", description="Build year."
            ),
            "scenario": Dimension(dtype=nw.String(), description="One realisation."),
        },
        attributes={
            "p_nom": AttributeSpec(
                dtype=nw.Float64(), unit="MW", description="Nominal power."
            )
        },
    )
    assert s.dimensions["vintage"].unit == "year"
    assert s.attributes["p_nom"].unit == "MW"
    # An axis whose labels are not a quantity declares none.
    assert s.dimensions["scenario"].unit is None


def test_undeclared_is_none_not_empty():
    """`None` is "undeclared", `""` is "genuinely dimensionless".

    Notes
    -----
    - [unit and description](https://energy-models.github.io/datarecord/design/schema/#unit-and-description)
    """
    assert AttributeSpec(dtype=nw.Float64()).unit is None
    assert AttributeSpec(dtype=nw.Float64(), unit="").unit == ""


def test_changing_a_unit_is_compatible():
    """Neither field decides how a row decodes, so editing one is compatible.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    old = _schema()
    new = _schema()
    spec = new.attributes["p_nom"]
    new.attributes["p_nom"] = spec.model_copy(
        update={"unit": "kW", "description": "Rated power."}
    )
    assert new.compatible_with(old) == []


# -- serialisation (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ----------------------------------------------------


def test_round_trips_through_json():
    """`manifest.json` is how a schema is written down, so this must be lossless."""
    s = _schema()
    back = Schema.model_validate_json(s.model_dump_json())
    assert back == s


def test_column_types_cover_structural_dims_and_flags():
    s = _schema()
    # A declared dim wins over the structural default, so the type axis carries
    # the `Enum` that pins its vocabulary rather than a bare string - and one a
    # schema calls `kind` is typed the same way.
    assert s.column_type("entity_type") == nw.Enum(["Generator", "Link"])
    assert s.column_type("entity") == nw.String()
    assert s.column_type("timestep") == nw.Datetime()
    # One struct per flag column, a BOOLEAN field per declared dim (https://energy-models.github.io/datarecord/design/read-path/#owner-map), so
    # the map's column set does not widen when a dim is declared.
    for column in ("varies", "broadcast"):
        column_type = s.column_type(column)
        assert column_type == flag_type(s.broadcast_dims)
        assert column_type is not None
        duck_types = DuckTypes(duckdb.connect())
        assert "scenario BOOLEAN" in str(duck_types(column_type))
    assert s.column_type("value") is None
    assert s.value_type("p_nom") == nw.Float64()


def test_attributes_need_at_least_one_dim():
    """Attribute data varying over no axis is a table, not a record.

    Rejected at the schema rather than handled in the fold: the owner map's
    flag columns are structs with a field per dim, and DuckDB has no empty
    struct - so forbidding the case is what keeps a placeholder field out of
    every fold.

    Notes
    -----
    - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
    """
    with pytest.raises(ValidationError, match="at least one dim"):
        Schema(attributes={"p_nom": AttributeSpec(dtype=nw.Float64())})


def test_a_schema_declaring_nothing_stays_legal():
    """`Schema()` is "no manifest yet", not a claim that there are no axes.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    assert Schema().dims == ()
    assert Schema().attributes == {}
