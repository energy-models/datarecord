"""The typed schema: declarations, derived keys, validation, versioning.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
"""

import pytest
from pydantic import ValidationError

from datarecord.schema import AttributeSpec, ComponentType, Dimension, Schema, flag_type


def _schema(**overrides) -> Schema:
    """A schema shaped like a stochastic multi-period record."""
    kwargs = {
        "dimensions": {
            "period": Dimension(dtype="BIGINT"),
            "timestep": Dimension(dtype="TIMESTAMP", within={"period"}),
            "scenario": Dimension(dtype="VARCHAR", keys={"component", "connection"}),
        },
        # Declared once, record-wide; a type subscribes to what it carries.
        "attributes": {
            "p_nom": AttributeSpec(dtype="DOUBLE"),
            "p_max_pu": AttributeSpec(dtype="DOUBLE", dims={"scenario", "timestep"}),
            "marginal_cost": AttributeSpec(
                dtype="DOUBLE", dims={"scenario"}, breakpoints=True
            ),
            "carrier": AttributeSpec(dtype="VARCHAR"),
            "efficiency": AttributeSpec(
                dtype="DOUBLE", dims={"scenario", "timestep"}, bus="connection"
            ),
        },
        "component_types": {
            "Generator": ComponentType(
                attributes={"p_nom", "p_max_pu", "marginal_cost", "carrier"}
            ),
            "Link": ComponentType(attributes={"efficiency"}),
        },
        "partial": frozenset({"scenario"}),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- derived keys (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override) -----------------------------------------------------


def test_ownership_is_derived_not_declared():
    """`owned_per` is `dims` and `partial` together, never a third declaration."""
    s = _schema()
    # Varies over both axes, but only `scenario` is partial - so a patch to one
    # timestep restates that scenario's whole series.
    assert s.owned_per("p_max_pu") == frozenset({"scenario"})
    assert s.owned_per("marginal_cost") == frozenset({"scenario"})
    # A first-stage decision: one value, owned once across everything.
    assert s.owned_per("p_nom") == frozenset()
    assert s.owned_per("carrier") == frozenset()


def test_a_scenario_varying_capacity_is_a_schema_violation():
    """`dims` is what forbids it: a capacity is decided before the scenario is known."""
    s = _schema()
    assert "scenario" not in s.attributes["p_nom"].dims
    # Nothing owns it per scenario, so the fold writes NULL there and one value
    # applies to every scenario.
    assert s.owned_per("p_nom") == frozenset()


def test_partial_dims_is_the_union_over_attributes():
    """The fold's key is one fixed tuple, so an unowned dim is NULL rather than absent."""
    s = _schema()
    assert s.partial_dims == ("scenario",)

    # Make `timestep` partial too and it joins the key.
    wider = _schema(partial=frozenset({"scenario", "timestep"}))
    assert wider.partial_dims == ("period", "timestep", "scenario")[1:]


def test_file_split_follows_dims():
    """Varying over nothing is what puts an attribute in `dims/components/`.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    s = _schema()
    assert not s.attributes["p_nom"].varying
    assert not s.attributes["carrier"].varying
    assert s.attributes["p_max_pu"].varying


# -- membership keys (https://energy-models.github.io/datarecord/design/schema/#keys-which-entity-tables-a-dim-keys) --------------------------------------------------


def test_a_non_broadcast_dim_must_be_partial():
    """Addressed individually and patchable value by value are the same fact.

    A NULL `entity` is a value belonging to no component, not to all of them,
    so a layer setting one component's value patches that entity alone - which
    is what `partial` declares, and what keys the fold's ownership.
    """
    # Built directly rather than through `_schema`, which supplies the
    # requirement - the point here is a schema that does not.
    with pytest.raises(ValidationError, match="do not broadcast"):
        Schema(
            dimensions={
                "entity": Dimension(dtype="VARCHAR"),
                "scenario": Dimension(dtype="VARCHAR"),
            },
            partial=frozenset({"scenario"}),
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
            "horizon": Dimension(dtype="BIGINT"),
            "period": Dimension(dtype="BIGINT", within={"horizon"}),
            "timestep": Dimension(dtype="TIMESTAMP", within={"period"}),
        }
    )
    assert s.axis_key("timestep") == ("horizon", "period", "timestep")


def test_several_direct_parents():
    """A set, since two axes may each qualify a label without containing each other."""
    s = Schema(
        dimensions={
            "period": Dimension(dtype="BIGINT"),
            "stage": Dimension(dtype="VARCHAR"),
            "timestep": Dimension(dtype="TIMESTAMP", within={"period", "stage"}),
        }
    )
    assert s.axis_key("timestep") == ("period", "stage", "timestep")


def test_nesting_must_name_declared_dims():
    with pytest.raises(ValidationError, match="undeclared"):
        Schema(dimensions={"timestep": Dimension(dtype="TIMESTAMP", within={"nope"})})


def test_nesting_must_be_acyclic():
    with pytest.raises(ValidationError, match="cyclic"):
        Schema(
            dimensions={
                "a": Dimension(dtype="BIGINT", within={"b"}),
                "b": Dimension(dtype="BIGINT", within={"a"}),
            }
        )


def test_a_dim_cannot_be_within_itself():
    with pytest.raises(ValidationError, match="within` itself"):
        Schema(dimensions={"a": Dimension(dtype="BIGINT", within={"a"})})


def test_an_attribute_cannot_vary_over_an_undeclared_dim():
    with pytest.raises(ValidationError, match="undeclared"):
        Schema(
            dimensions={"scenario": Dimension(dtype="VARCHAR")},
            attributes={"p": AttributeSpec(dtype="DOUBLE", dims={"nope"})},
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
        dimensions={"scenario": Dimension(dtype="VARCHAR")},
        attributes={"p_nom_max": AttributeSpec(dtype="DOUBLE", default=value)},
    )
    back = Schema.model_validate_json(schema.model_dump_json())
    assert repr(back.attributes["p_nom_max"].default) == repr(value)


# -- versioning (https://energy-models.github.io/datarecord/design/schema/#versioning) -------------------------------------------------------


def test_adding_an_attribute_is_compatible():
    old = _schema()
    new = _schema()
    new.attributes["p_min_pu"] = AttributeSpec(dtype="DOUBLE", dims={"scenario"})
    assert new.compatible_with(old) == []


def test_widening_dims_is_compatible():
    """Rows that set fewer dims still decode: an unset dim is NULL, and NULL means all."""
    old = _schema()
    new = _schema()
    new.attributes["marginal_cost"] = AttributeSpec(
        dtype="DOUBLE", dims={"scenario", "timestep"}, breakpoints=True
    )
    assert new.compatible_with(old) == []


def test_widening_partial_is_compatible():
    """Ownership becomes finer; an old row is owned at the coarser granularity."""
    old = _schema()
    new = _schema(partial=frozenset({"scenario", "timestep"}))
    assert new.compatible_with(old) == []


def test_narrowing_dims_is_incompatible():
    old = _schema()
    new = _schema()
    new.attributes["p_max_pu"] = AttributeSpec(dtype="DOUBLE", dims={"scenario"})
    (reason,) = new.compatible_with(old)
    assert "no longer varies over ['timestep']" in reason


def test_changing_a_dtype_is_incompatible():
    old = _schema()
    new = _schema()
    new.attributes["p_nom"] = AttributeSpec(dtype="BIGINT")
    (reason,) = new.compatible_with(old)
    assert "DOUBLE -> BIGINT" in reason


def test_removing_from_partial_is_incompatible():
    """A layer that patched one value is now a partial override of a whole axis."""
    old = _schema(partial=frozenset({"scenario", "timestep"}))
    new = _schema()
    reasons = new.compatible_with(old)
    assert any("no longer `partial`" in r for r in reasons)


def test_changing_nesting_is_incompatible():
    old = _schema()
    new = _schema(
        dimensions={
            "period": Dimension(dtype="BIGINT"),
            "timestep": Dimension(dtype="TIMESTAMP"),
            "scenario": Dimension(dtype="VARCHAR", keys={"component", "connection"}),
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
                dtype="BIGINT", unit="year", description="Build year."
            ),
            "scenario": Dimension(dtype="VARCHAR", description="One realisation."),
        },
        attributes={
            "p_nom": AttributeSpec(
                dtype="DOUBLE", unit="MW", description="Nominal power."
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
    assert AttributeSpec(dtype="DOUBLE").unit is None
    assert AttributeSpec(dtype="DOUBLE", unit="").unit == ""


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
    assert s.column_type("component_type") == "VARCHAR"
    assert s.column_type("timestep") == "TIMESTAMP"
    # One struct per flag column, a BOOLEAN field per declared dim (https://energy-models.github.io/datarecord/design/read-path/#owner-map), so
    # the map's column set does not widen when a dim is declared.
    for column in ("varies", "broadcast"):
        column_type = s.column_type(column)
        assert column_type == flag_type(s.dims)
        assert column_type is not None
        assert '"scenario" BOOLEAN' in column_type
    assert s.column_type("value") is None
    assert s.value_type("p_nom") == "DOUBLE"


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
        Schema(attributes={"p_nom": AttributeSpec(dtype="DOUBLE")})


def test_a_schema_declaring_nothing_stays_legal():
    """`Schema()` is "no manifest yet", not a claim that there are no axes.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    assert Schema().dims == ()
    assert Schema().attributes == {}
