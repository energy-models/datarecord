# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The typed schema: declarations, derived keys, validation, versioning.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
"""

import json
from functools import reduce
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
    PropertySpec,
    Schema,
    flag_type,
)
from tests.fixtures import schema as pypsa_schema


def _schema(**overrides) -> Schema:
    """A schema shaped like a stochastic multi-period record."""
    kwargs: dict[str, Any] = {
        "dimensions": {
            "entity": Dimension(dtype=nw.String()),
            "bus": Dimension(dtype=nw.String()),
            "period": Dimension(dtype=nw.Int64()),
            "timestep": Dimension(dtype=nw.Datetime()),
            "scenario": Dimension(dtype=nw.String()),
        },
        "properties": {
            "entity": {"ctype": PropertySpec(dtype=nw.Enum(["Generator", "Link"]))}
        },
        "groups": {"connection": Group(over={"entity": "entity", "bus": "bus"})},
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
        "partial": frozenset({"entity", "scenario"}),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- derived keys (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override) -----------------------------------------------------


def test_ownership_is_derived_not_declared():
    """`owned_per` is `dims` and `partial` together, never a third declaration."""
    s = _schema()
    # Varies over both time axes, but only `scenario` is partial among them -
    # so a patch to one timestep restates that scenario's whole series.
    # `entity` joins it because a layer patches one component's value.
    assert s.owned_per("p_max_pu") == frozenset({"entity", "scenario"})
    assert s.owned_per("marginal_cost") == frozenset({"entity", "scenario"})
    # A first-stage decision: one value per component, owned once across
    # every axis it does not vary over.
    assert s.owned_per("p_nom") == frozenset({"entity"})
    assert s.owned_per("carrier") == frozenset({"entity"})


def test_partial_dims_is_the_union_over_attributes():
    """The fold's key is one fixed tuple, so an unowned dim is NULL rather than absent."""
    s = _schema()
    assert s.partial_dims == ("entity", "scenario"), "only what `partial` lists"

    wider = _schema(partial=frozenset({"entity", "scenario", "timestep", "connection"}))
    assert wider.partial_dims == ("entity", "timestep", "scenario", "connection"), (
        "a `partial` dim or group joins the key"
    )

    dims = {
        "entity": Dimension(dtype=nw.String()),
        "scenario": Dimension(dtype=nw.String()),
    }
    unlisted = Schema(dimensions=dims, partial=frozenset({"scenario"}))
    assert unlisted.partial_dims == ("scenario",), (
        "no dim is keyed by name: `entity` is owned whole where not partial"
    )


# -- addressing (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives) ------------------------------------


@pytest.mark.parametrize(
    ("attribute", "addresses"),
    [("p_nom", True), ("efficiency", True), ("weighting", False)],
    ids=["over-entity", "over-a-group-drawing-on-it", "over-another-dim"],
)
def test_an_attribute_addresses_entity_through_its_dims_or_a_group(
    attribute, addresses
):
    """`efficiency` is over `connection`, whose coordinates include `entity`."""
    s = _schema(
        attributes={
            **_schema().attributes,
            "weighting": AttributeSpec(dtype=nw.Float64(), dims={"timestep"}),
        }
    )
    assert s.addresses(attribute, "entity") is addresses


def _with_entity_property(name: str, spec: PropertySpec) -> dict:
    """`_schema`'s properties with `name` added to `entity`'s."""
    return {"entity": {**_schema().properties["entity"], name: spec}}


def test_a_functional_group_may_not_key_an_attribute_with_what_it_maps_from():
    """`into` says the label follows from the key, so the row is keyed twice."""
    with pytest.raises(ValidationError, match="keys a row twice over"):
        Schema(
            dimensions={
                "bus": Dimension(dtype=nw.String()),
                "scenario": Dimension(dtype=nw.String()),
                "country": Dimension(dtype=nw.String()),
            },
            groups={"in_country": Group(over=["bus", "scenario"], into="country")},
            attributes={
                "x": AttributeSpec(dtype=nw.Float64(), dims={"bus", "country"})
            },
            partial=frozenset(),
        )


def test_the_section_decides_where_a_value_lives():
    """An attribute is long rows whatever its `dims`; a property is a column of its subject's axis.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    s = _schema()
    assert s.long_columns_for("p_nom") == ("entity", "attribute", "breakpoint", "value")
    assert s.column_type("p_nom") is None, "a single-dim attribute is no axis column"
    assert s.column_type("ctype", "entity") == nw.Enum(["Generator", "Link"])
    assert s.column_type("ctype") is None, "a column of its subject's file only"
    assert s.categorizing("entity") == ("ctype",)
    assert "ctype" not in s.long_columns
    assert "ctype" not in s.broadcast_dims
    assert "ctype" not in s.partial_dims


def test_a_group_may_have_properties():
    """A column of `groups/<group>.parquet`, typed like a dim's property."""
    s = _schema(
        properties={
            "entity": {"ctype": PropertySpec(dtype=nw.Enum(["Generator", "Link"]))},
            "connection": {"role": PropertySpec(dtype=nw.Enum(["in", "out"]))},
        },
        partial=frozenset({"entity", "scenario", "connection"}),
    )
    assert s.resolve("connection.role") == (
        "connection",
        s.properties["connection"]["role"],
    )
    assert s.column_type("role", "connection") == nw.Enum(["in", "out"])
    assert s.categorizing("connection") == (), (
        "a group is never categorized, even by an `Enum` property of a `partial` group"
    )


def test_an_enum_property_categorizes_only_a_partial_dim():
    """No flag: the dtype and `partial` decide, a `String` property never."""
    s = _schema(
        properties={
            "entity": {
                **_schema().properties["entity"],
                "carrier_kind": PropertySpec(dtype=nw.Enum(["wind", "solar"])),
                "label": PropertySpec(dtype=nw.String()),
            },
            "period": {"era": PropertySpec(dtype=nw.Enum(["old", "new"]))},
        }
    )
    assert s.categorizing("entity") == ("ctype", "carrier_kind"), (
        "every `Enum` property, in declaration order"
    )
    assert s.categorizing("period") == (), "`period` is not partial"
    assert s.categorizing("scenario") == (), "no properties"


@pytest.mark.parametrize(
    ("build", "match"),
    [
        pytest.param(
            lambda: _schema(
                properties=_with_entity_property(
                    "entity", PropertySpec(dtype=nw.String())
                )
            ),
            "named like a column of its key",
            id="its-dims-name",
        ),
        pytest.param(
            lambda: _schema(
                properties={
                    "connection": {"connection": PropertySpec(dtype=nw.String())}
                }
            ),
            "named like a column of its key",
            id="its-groups-name",
        ),
        pytest.param(
            lambda: Schema(
                dimensions={
                    "bus": Dimension(dtype=nw.String()),
                    "scenario": Dimension(dtype=nw.String()),
                    "country": Dimension(dtype=nw.String()),
                },
                groups={"bus_country": Group(over=["bus", "scenario"], into="country")},
                properties={
                    "bus_country": {"country": PropertySpec(dtype=nw.String())}
                },
            ),
            "named like a column of its key",
            id="its-groups-into",
        ),
        pytest.param(
            lambda: _schema(
                properties=_with_entity_property(
                    "P_nom", PropertySpec(dtype=nw.Float64())
                )
            ),
            "differ only in case",
            id="an-attributes-name-in-another-case",
        ),
        pytest.param(
            lambda: _schema(
                properties=_with_entity_property(
                    "a.b", PropertySpec(dtype=nw.Float64())
                )
            ),
            "contains `.`",
            id="a-dotted-name",
        ),
        pytest.param(
            lambda: Schema(
                dimensions={"bus": Dimension(dtype=nw.String())},
                groups={"corridor": Group(over={"from": "bus", "to": "bus"})},
                properties={"corridor": {"from": PropertySpec(dtype=nw.String())}},
            ),
            "share their names with its coordinates",
            id="a-group-coordinates-name",
        ),
        pytest.param(
            lambda: _schema(
                properties={"nope": {"kind": PropertySpec(dtype=nw.String())}}
            ),
            "undeclared dims or groups",
            id="on-an-undeclared-dim",
        ),
    ],
)
def test_a_property_is_one_column_of_a_declared_subject(build, match):
    """A property's column may collide with no other name, and lives on its own subject."""
    with pytest.raises(ValidationError, match=match):
        build()


def _shared() -> Schema:
    """`_schema` with names its sections share: `p_nom`, `x`, `bus` and `carrier`."""
    return _schema(
        properties={
            "entity": {
                **_schema().properties["entity"],
                "bus": PropertySpec(dtype=nw.String()),
                "x": PropertySpec(dtype=nw.Float64()),
            },
            "bus": {
                "carrier": PropertySpec(dtype=nw.String()),
                "p_nom": PropertySpec(dtype=nw.Int64()),
                "x": PropertySpec(dtype=nw.Float64()),
            },
        }
    )


@pytest.mark.parametrize(
    ("name", "subject", "dtype"),
    [
        ("p_nom", None, nw.Float64()),
        ("bus.p_nom", "bus", nw.Int64()),
        ("carrier", None, nw.String()),
        ("bus.carrier", "bus", nw.String()),
        ("entity.bus", "entity", nw.String()),
    ],
)
def test_resolve_reads_a_property_dotted_and_a_plain_name_as_an_attribute(
    name, subject, dtype
):
    """A property shares its name with an attribute, a dim or another subject's property.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
    """
    resolved = _shared().resolve(name)
    spec = resolved[1] if isinstance(resolved, tuple) else resolved
    assert (resolved[0] if isinstance(resolved, tuple) else None) == subject
    assert spec.dtype == dtype


@pytest.mark.parametrize(
    ("name", "match"),
    [
        ("x", "did you mean `bus.x` or `entity.x`"),
        ("bus", "did you mean `entity.bus`"),
        ("nope", "no attribute or result 'nope'\"$"),
        ("bus.nope", "no property 'bus.nope'"),
        ("entity.p_nom", "no property 'entity.p_nom'"),
    ],
)
def test_resolve_refuses_an_unknown_name_suggesting_dotted_properties(name, match):
    with pytest.raises(KeyError, match=match):
        _shared().resolve(name)


def test_a_record_may_declare_no_category_at_all():
    """Categories are optional: with none, no keyword selects labels of a dim.

    A tool needing them requires the property in the schema it builds, which
    is where that requirement belongs.
    """
    s = Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "timestep": Dimension(dtype=nw.Datetime()),
        },
        attributes={
            "p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
            "weighting": AttributeSpec(dtype=nw.Float64(), dims={"timestep"}),
        },
        partial=frozenset(),
    )
    assert s.categorizing("entity") == (), "no vocabulary declared"
    with pytest.raises(ValueError, match="do not categorize 'entity'"):
        s.require_categorizing("entity", ["ctype"])


# -- defaults by label -------------------------------------------------------


def test_default_for_reads_a_labels_default_over_the_plain_one():
    s = _schema(
        attributes={
            **_schema().attributes,
            "sign": AttributeSpec(
                dtype=nw.Float64(),
                dims={"entity"},
                default=1.0,
                default_by={"entity.ctype": {"Link": -1.0}},
            ),
        }
    )
    assert s.default_for("sign", ctype="Link") == -1.0
    assert s.default_for("sign", ctype="Generator") == 1.0, (
        "no entry, so the plain default"
    )
    assert s.default_for("sign") == 1.0
    assert s.default_for("entity.ctype") is None, "a property has one too"
    with pytest.raises(KeyError):
        s.default_for("nope")

    plain = _schema(
        properties={
            "entity": {
                "ctype": PropertySpec(
                    dtype=nw.Enum(["Generator", "Link"]), default="Generator"
                )
            }
        }
    )
    assert plain.default_for("entity.ctype") == "Generator", (
        "a categorizing property may have a plain default"
    )


def _with_x(spec: AttributeSpec, base=_schema) -> Schema:
    """`base()` with `spec` declared as attribute `x`."""
    return base(attributes={**base().attributes, "x": spec})


@pytest.mark.parametrize(
    ("build", "match"),
    [
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"entity"},
                    default_by={"kind": {"Link": 1.0}},
                )
            ),
            "which is no property",
            id="no-property",
        ),
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"entity"},
                    default_by={"entity.ctype": {"Store": 1.0}},
                )
            ),
            "not categories of 'entity.ctype'",
            id="an-unknown-category",
        ),
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"timestep"},
                    default_by={"entity.ctype": {"Link": 1.0}},
                )
            ),
            "does not reach its dim 'entity'",
            id="a-dim-out-of-reach",
        ),
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"entity"},
                    default_by={
                        "entity.ctype": {"Link": 0.5},
                        "bus.bus_type": {"DC": 0.0},
                    },
                ),
                base=_two_subjects,
            ),
            "does not reach its dim 'bus'",
            id="every-key-reaches-its-dim",
        ),
        pytest.param(
            lambda: _schema(
                properties={
                    "entity": {
                        "ctype": PropertySpec(
                            dtype=nw.Enum(["Generator", "Link"]),
                            default="Generator",
                            default_by={"entity.ctype": {"Link": "Generator"}},
                        )
                    }
                }
            ),
            "circular",
            id="a-categorizing-property-by-a-category",
        ),
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"entity"},
                    default_by={"entity.kind": {"wind": 1.0}},
                ),
                base=lambda **kw: _schema(
                    properties=_with_entity_property(
                        "kind", PropertySpec(dtype=nw.String())
                    ),
                    **kw,
                ),
            ),
            "is no `Enum` property",
            id="a-string-property",
        ),
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"period"},
                    default_by={"period.era": {"old": 1.0}},
                ),
                base=lambda **kw: _schema(
                    properties={
                        **_schema().properties,
                        "period": {"era": PropertySpec(dtype=nw.Enum(["old", "new"]))},
                    },
                    **kw,
                ),
            ),
            "`partial` does not list",
            id="a-property-of-a-non-partial-dim",
        ),
        pytest.param(
            lambda: _with_x(
                AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"connection"},
                    default_by={"connection.role": {"in": 1.0}},
                ),
                base=lambda **kw: _schema(
                    properties={
                        **_schema().properties,
                        "connection": {
                            "role": PropertySpec(dtype=nw.Enum(["in", "out"]))
                        },
                    },
                    partial=frozenset({"entity", "scenario", "connection"}),
                    **kw,
                ),
            ),
            "a group is never categorized",
            id="a-group-property",
        ),
    ],
)
def test_default_by_is_validated_against_the_categories(build, match):
    with pytest.raises(ValidationError, match=match):
        build()


def _two_subjects(**overrides) -> Schema:
    """`_schema` with `bus` categorized by `bus_type` beside `ctype`."""
    kwargs: dict[str, Any] = {
        "properties": {
            **_schema().properties,
            "bus": {"bus_type": PropertySpec(dtype=nw.Enum(["AC", "DC"]))},
        },
        "attributes": {
            **_schema().attributes,
            "v_nom": AttributeSpec(dtype=nw.Float64(), dims={"bus"}),
            "frequency": AttributeSpec(dtype=nw.Float64(), dims={"bus"}),
        },
        "partial": frozenset({"entity", "bus", "scenario"}),
    }
    kwargs.update(overrides)
    return _schema(**kwargs)


def test_a_category_on_bus_works_like_one_on_entity():
    """A `default_by` keyed by `bus_type` defaults bus attributes alone."""
    s = _two_subjects(
        attributes={
            **_two_subjects().attributes,
            "frequency": AttributeSpec(
                dtype=nw.Float64(),
                dims={"bus"},
                default=0.0,
                default_by={"bus.bus_type": {"AC": 50.0}},
            ),
        }
    )
    assert s.default_for("frequency", bus_type="AC") == 50.0
    assert s.default_for("frequency", bus_type="DC") == 0.0
    assert s.categorizing("bus") == ("bus_type",)
    assert s.categorizing("entity") == ("ctype",)
    assert not s.addresses("frequency", "entity"), "a bus attribute reaches no entity"


def test_default_by_takes_the_first_key_listing_a_category():
    """Several keys may default one value; declaration order decides between them."""
    loss = AttributeSpec(
        dtype=nw.Float64(),
        dims={"connection"},
        default=1.0,
        default_by={"bus.bus_type": {"DC": 0.0}, "entity.ctype": {"Link": 0.5}},
    )
    s = _two_subjects(attributes={**_two_subjects().attributes, "loss": loss})
    assert s.default_for("loss", bus_type="DC", ctype="Link") == 0.0, (
        "`bus_type` is declared first"
    )
    assert s.default_for("loss", bus_type="AC", ctype="Link") == 0.5, (
        "`bus_type` lists no AC, so `ctype` decides"
    )
    assert s.default_for("loss", bus_type="AC", ctype="Generator") == 1.0
    back = Schema.model_validate_json(s.model_dump_json())
    assert list(back.attributes["loss"].default_by) == [
        "bus.bus_type",
        "entity.ctype",
    ], "the order survives `manifest.json`"


@pytest.mark.parametrize(
    ("build", "match"),
    [
        pytest.param(
            lambda: Schema(
                dimensions={"scenario": Dimension(dtype=nw.String())},
                attributes={"p": AttributeSpec(dtype=nw.Float64(), dims={"nope"})},
            ),
            "undeclared",
            id="an-attribute-over-an-undeclared-dim",
        ),
    ],
)
def test_a_dim_reference_must_resolve(build, match):
    """An attribute's `dims` name declared dims."""
    with pytest.raises(ValidationError, match=match):
        build()


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


def _restating(**attributes: AttributeSpec) -> Schema:
    """`_schema` with `attributes` added or replaced, every other one unchanged."""
    return _schema(attributes={**_schema().attributes, **attributes})


@pytest.mark.parametrize(
    "new",
    [
        pytest.param(
            _restating(p_min_pu=AttributeSpec(dtype=nw.Float64(), dims={"scenario"})),
            id="adding-an-attribute",
        ),
        pytest.param(
            _restating(
                marginal_cost=AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"entity", "scenario", "timestep"},
                    breakpoints=True,
                )
            ),
            id="widening-dims",
        ),
        pytest.param(
            _schema(partial=frozenset({"entity", "scenario", "timestep"})),
            id="widening-partial",
        ),
        pytest.param(
            _restating(
                p_nom=AttributeSpec(
                    dtype=nw.Float64(),
                    dims={"entity"},
                    unit="kW",
                    description="Rated power.",
                )
            ),
            id="changing-a-unit",
        ),
    ],
)
def test_a_compatible_change(new):
    """Every row the old schema wrote still decodes under the new one.

    A row setting fewer dims reads an unset dim as NULL, which means all; an
    old row is owned at the coarser granularity; and neither `unit` nor
    `description` decides how a row decodes.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    assert new.compatible_with(_schema()) == []


@pytest.mark.parametrize(
    ("old", "new", "reason"),
    [
        pytest.param(
            _schema(),
            _restating(
                p_max_pu=AttributeSpec(dtype=nw.Float64(), dims={"entity", "scenario"})
            ),
            "no longer varies over ['timestep']",
            id="narrowing-dims",
        ),
        pytest.param(
            _schema(),
            _restating(p_nom=AttributeSpec(dtype=nw.Int64(), dims={"entity"})),
            "Float64 -> Int64",
            id="changing-a-dtype",
        ),
        pytest.param(
            _schema(partial=frozenset({"entity", "scenario", "timestep"})),
            _schema(),
            "no longer `partial`",
            id="removing-from-partial",
        ),
    ],
)
def test_an_incompatible_change(old, new, reason):
    """A row the old schema wrote has no valid reading under the new one.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    reasons = new.compatible_with(old)
    assert any(reason in r for r in reasons), reasons


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
    assert AttributeSpec(dtype=nw.Float64(), unit="").unit == "", (
        '`""` is "genuinely dimensionless", apart from `None`, "undeclared"'
    )


# -- serialisation (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ----------------------------------------------------


def test_round_trips_through_json():
    """`manifest.json` is how a schema is written down, so this must be lossless."""
    s = _schema()
    back = Schema.model_validate_json(s.model_dump_json())
    assert back == s


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("dimensions", "snapshot"),
        ("attributes", "p_nom"),
        ("properties", "entity", "ctype"),
        ("groups", "connection"),
    ],
    ids=["top-level", "dim", "attribute", "property", "group"],
)
def test_manifest_with_unknown_key_fails_to_load(path: tuple[str, ...]):
    s = pypsa_schema(
        attributes={"Generator": {"p_nom": AttributeSpec(dtype=nw.Float64())}}
    )
    manifest = json.loads(s.model_dump_json())
    reduce(dict.__getitem__, path, manifest)["misspelt"] = 1
    with pytest.raises(ValidationError, match="misspelt"):
        Schema.model_validate_json(json.dumps(manifest))


def test_column_types_cover_structural_dims_and_flags():
    s = _schema()
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


# -- names (https://energy-models.github.io/datarecord/design/schema/#names) -----


def _declaring(position: str, name: str) -> Schema:
    """A minimal schema declaring `name` in one position."""
    dim = Dimension(dtype=nw.String())
    attr = AttributeSpec(dtype=nw.Float64(), dims={"entity"})
    if position == "dim":
        return Schema(dimensions={"entity": dim, name: dim}, partial={name})
    if position == "group":
        return Schema(
            dimensions={"entity": dim, "bus": dim},
            groups={name: Group(over=["entity", "bus"])},
        )
    if position == "coordinate":
        return Schema(
            dimensions={"entity": dim, "bus": dim},
            groups={"connection": Group(over={"entity": "entity", name: "bus"})},
        )
    if position == "attribute":
        return Schema(dimensions={"entity": dim}, attributes={name: attr})
    return Schema(dimensions={"entity": dim}, results={name: attr})


POSITIONS = ["dim", "group", "coordinate", "attribute", "result"]

WRITTEN = {
    "attribute": "a column datarecord writes",
    "breakpoint": "a column datarecord writes",
    "value": "a column datarecord writes",
    "deleted": "a column datarecord writes",
    "breakpoints": "a column datarecord writes",
    "varies": "a column datarecord writes",
    "broadcast": "a column datarecord writes",
    "layer_uuid": "a column datarecord writes",
    "order_key": "a column datarecord writes",
    "Value": "a column datarecord writes",
    "_depth": "starts with `_`",
    "_row": "starts with `_`",
    "_rank": "starts with `_`",
    "_first": "starts with `_`",
    "_broadcast": "starts with `_`",
    "_pos": "starts with `_`",
    'say "hi"': 'contains `"`',
    "a.b": "contains `.`",
}


@pytest.mark.parametrize("position", POSITIONS)
@pytest.mark.parametrize("name", list(WRITTEN))
def test_a_name_that_cannot_be_its_own_column_is_refused(name, position):
    """Every column the format or the fold writes beside a schema's own, `.` and
    `"`, which `duckdb.ColumnExpression` cannot spell, and case folded as DuckDB
    folds it. `deleted` as a dim read every label as a tombstone, and `value`
    gave the long file two columns of one name."""
    with pytest.raises(ValidationError) as refused:
        _declaring(position, name)
    message = str(refused.value)
    assert repr(name) in message, "the error names the offending name"
    assert WRITTEN[name] in message, "the error says why the name is refused"
    assert "Rename" in message, "the error says what to do"


@pytest.mark.parametrize("position", POSITIONS)
def test_names_differing_only_in_case_are_refused(position):
    """DuckDB resolves `Entity` and `entity` to one column, so declaring both
    would have one read the other's values."""
    with pytest.raises(ValidationError) as refused:
        _declaring(position, "Entity")
    message = str(refused.value)
    assert "'Entity'" in message, "the error names the new spelling"
    assert "'entity'" in message, "and the one it clashes with"
    assert "differ only in case" in message, "the error says why"


@pytest.mark.parametrize("position", POSITIONS)
@pytest.mark.parametrize("name", ["Order Size", "select", "Horizon", "1st"])
def test_a_name_duckdb_quotes_is_accepted(name, position):
    """A space, a keyword, mixed case or a leading digit all quote cleanly."""
    _declaring(position, name)
