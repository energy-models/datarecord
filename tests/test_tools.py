"""The tool layer: verify a record, build a model, read results back (design doc §12)."""

from pathlib import Path
from types import SimpleNamespace

import narwhals as nw
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.layered.write import write_record
from datarecord.tools.base import Requirements, Schema, UnsupportedRecordError
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    export_network,
    relation,
    schema,
    write_components,
    write_input,
)


@pytest.fixture
def single_record(con, base_uri, ac_dc):
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    return record


def test_requirements_falsy_when_empty():
    """`bool(Requirements())` distinguishes "nothing missing" from "nothing checked"."""
    assert not Requirements()
    assert Requirements(dims=frozenset({"snapshot"}))
    assert "dims ['snapshot']" in Requirements(dims=frozenset({"snapshot"})).describe()
    assert Requirements().describe() == "nothing"


def test_the_record_layer_imports_no_tool():
    """`datarecord` is framework-free: reading a record never imports PyPSA.

    The call runs from the tool inward (`PyPSA.build(record.store)`), so there is no
    registry and no name dispatch to drag a framework in (§12).
    """
    import subprocess
    import sys

    # A fresh interpreter, so an already-imported PyPSA from another test
    # cannot mask a real import edge.
    code = (
        "import sys; import datarecord; import datarecord.tools; "
        "assert 'pypsa' not in sys.modules, sorted(sys.modules)"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


def test_verify_accepts_a_complete_record(single_record):
    """A store written by `export_to_parquet` supplies everything PyPSA needs."""
    assert not PyPSA.verify(single_record.store)


def test_requires_reports_the_records_own_types(single_record):
    """`requires` is record-dependent: PyPSA's axes plus this record's types."""
    req = PyPSA.requires(single_record.store)
    assert {"snapshot", "period", "scenario"} <= req.dims
    assert "Generator" in req.component_types
    # Required attributes come from PyPSA's registry, not a list we maintain.
    assert ("Generator", "bus") in req.attributes
    assert ("Line", "x") in req.attributes
    # `name` is the member identity, never an attribute row.
    assert not [a for a in req.attributes if a[1] == "name"]


def test_verify_reports_a_missing_dim(con, base_uri, ac_dc):
    """A schema that declares no `scenario` dim cannot build a network (§12)."""
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    _with_schema(record, dims={"snapshot": "TIMESTAMP"}, partial=set(), keys={})

    missing = PyPSA.verify(record.store)
    assert missing
    assert missing.dims == {"period", "scenario"}
    # A build refuses rather than failing deep inside PyPSA.
    with pytest.raises(UnsupportedRecordError, match="scenario"):
        PyPSA.build(record.store)


def test_verify_reports_a_type_the_tool_does_not_know(con, base_uri, ac_dc):
    """A type outside PyPSA's registry is reported by `verify`, not raised in the fold.

    The record layer stores `component_type` as a plain `VARCHAR` - the
    vocabulary belongs to a framework, and the record layer knows none - so an
    unknown type reads back fine and it is this tool's business that it cannot
    be built (§5, §12). `Requirements.component_types` is what carries it.
    """
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    write_components(layer_dir(record.id), "Widget", [{"name": "w1"}])

    missing = PyPSA.verify(record.store)
    assert missing.component_types == {"Widget"}
    # The types PyPSA does know are not reported.
    assert "Generator" not in missing.component_types
    with pytest.raises(UnsupportedRecordError, match="Widget"):
        PyPSA.build(record.store)


_DIMS = {"snapshot": "TIMESTAMP", "period": "BIGINT", "scenario": "VARCHAR"}


def _without_default(record, ctype: str, attribute: str) -> None:
    """Drop one attribute's declared default, leaving the rest of the schema."""
    was = read_schema()
    spec = was.attributes[ctype][attribute]
    was.attributes[ctype][attribute] = spec.model_copy(update={"default": None})
    write_schema(was)


def _with_schema(record, **kwargs) -> None:
    """Redeclare the store's schema, keeping the attributes it already declares.

    Written directly rather than through `write_record`, which would reject an
    incompatible redeclaration (§5.7) - here the point is to hand the tool a
    schema it must report on rather than one the writer accepted.
    """
    was = read_schema()
    now = schema(**kwargs).model_copy(
        update={"attributes": was.attributes, "meta": was.meta}
    )
    write_schema(now)


def test_verify_reports_a_snapshot_key(con, base_uri, ac_dc):
    """`snapshot` as a key is reported, not crashed on.

    A limit of this tool's *representation*, not of the layer format: PyPSA's
    static/series split needs a component's whole series for an attribute to
    come from one layer, so a stored `snapshot = NULL` broadcast row and a
    descendant's per-snapshot row would coexist with no single container to
    put the result in (§5.5). The record layer therefore permits the
    declaration - every file does carry the column - and the tool catches it.
    """
    record = Revision.create(con)
    write_schema(PyPSA.to_datarecord(ac_dc).schema)
    export_network(ac_dc, record, con)
    _with_schema(record, partial={"scenario", "snapshot"})

    missing = PyPSA.verify(record.store)
    assert ("input_key", "snapshot") in missing.unsupported_keys
    assert "snapshot" in missing.describe()
    with pytest.raises(UnsupportedRecordError, match="snapshot"):
        PyPSA.build(record.store)


@pytest.mark.parametrize(
    "kwargs",
    [
        # `period` keying components, which PyPSA's writer emits no column for.
        {
            "partial": {"scenario", "period"},
            "keys": {"scenario": {"component"}, "period": {"component"}},
        },
        # A dim no file has a column for at all.
        {"partial": {"scenario", "vintage"}, "dims": {**_DIMS, "vintage": "VARCHAR"}},
    ],
    ids=["period", "vintage"],
)
def test_write_record_rejects_a_key_dim_no_frame_carries(con, base_uri, ac_dc, kwargs):
    """A declared key dim needs a column in every frame, or the layer is refused.

    The invariant the read path relies on (§5.5): the fold keys by these
    columns, so a store missing one would resolve as though the dim were
    broadcast everywhere. Caught at the boundary, which is why no tool
    re-checks it.
    """
    source = PyPSA.to_datarecord(ac_dc)
    declared = schema(**kwargs).model_copy(
        update={"attributes": source.schema.attributes, "meta": source.schema.meta}
    )
    write_schema(declared)

    # The same frames, restated under a schema declaring the extra key.
    restated = SimpleNamespace(
        schema=declared,
        dims=source.dims,
        components=source.components,
        connections=source.connections,
        attributes=source.attributes,
    )
    record = Revision.create(con)
    with pytest.raises(ValueError, match="period|vintage"):
        write_record(record.id, restated, con)


def test_verify_reports_a_missing_required_attribute(con, base_uri, ac_dc):
    """A component type with no `bus` anywhere - not in the frame, not in the catalog."""
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    # A Generator member carrying no `bus` column at all, no connection row
    # supplying one (§6), and a schema with no default for it either.
    write_components(layer_dir(record.id), "Generator", [{"name": "g1"}])
    Path(layer_dir(record.id), "dims", "connections", "Generator.parquet").unlink()
    _without_default(record, "Generator", "bus")

    missing = PyPSA.verify(record.store)
    assert ("Generator", "bus") in missing.attributes


def test_verify_accepts_a_declared_default_for_a_required_attribute(
    con, base_uri, ac_dc
):
    """A declared default makes an attribute resolvable with no row anywhere (§5.2)."""
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    write_components(layer_dir(record.id), "Generator", [{"name": "g1"}])

    # PyPSA's own registry already declares `bus` with a `""` default, which
    # is exactly the case this pins - so the store is left as written.
    assert ("Generator", "bus") not in PyPSA.verify(record.store).attributes


def test_verify_reports_a_piecewise_linear_attribute(con, base_uri, ac_dc):
    """A curve is stored correctly; it is the PyPSA translation that cannot express it (§7)."""
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    write_input(
        layer_dir(record.id),
        "marginal_cost",
        [
            {
                "component_type": "Generator",
                "name": "Manchester Wind",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 20.0), (50.0, 35.0))
        ],
    )

    missing = PyPSA.verify(record.store)
    assert ("Generator", "marginal_cost") in missing.unsupported_values
    assert "piecewise-linear" in missing.describe()
    with pytest.raises(UnsupportedRecordError):
        PyPSA.build(record.store)


def test_verify_accepts_a_scalar_attribute(single_record):
    """The same attribute without breakpoints is not reported (the negative half)."""
    write_input(
        layer_dir(single_record.id),
        "marginal_cost",
        [{"component_type": "Generator", "name": "Manchester Wind", "value": 20.0}],
    )
    assert not PyPSA.verify(single_record.store).unsupported_values


def test_pypsa_schema_is_the_identity(single_record):
    """PyPSA defines the record vocabulary today, so nothing is renamed (§12).

    The seam still routes every attribute, so an entry added later takes
    effect with no change to `build`.
    """
    assert PyPSA.schema.attrs == {}
    assert PyPSA.schema.sources("Generator", "p_max_pu") == ("p_max_pu",)
    identity = PyPSA.schema.resolve(single_record.store, "Generator", "p_max_pu")
    assert identity.fetchall() == relation(single_record, "p_max_pu").fetchall()


def test_schema_renames_and_computes():
    """A rename maps one source; a computed attribute maps several (`Attr.compute`)."""
    from datarecord.tools.base import Attr

    schema = Schema(
        {
            "Generator": (
                Attr(name="p_max_pu", source=("availability",)),
                Attr(
                    name="marginal_cost",
                    source=("fuel_cost", "efficiency"),
                    compute=lambda fuel, eff: fuel,
                ),
            )
        }
    )
    assert schema.sources("Generator", "p_max_pu") == ("availability",)
    assert schema.sources("Generator", "marginal_cost") == ("fuel_cost", "efficiency")
    # Anything unmapped, and any other type, stays the identity.
    assert schema.sources("Generator", "p_nom") == ("p_nom",)
    assert schema.sources("Link", "p_max_pu") == ("p_max_pu",)
    # A multi-source rename with no way to combine them is a declaration bug.
    with pytest.raises(ValueError, match="exactly one"):
        Attr(name="x", source=("a", "b"))


def test_results_extracts_long_form_outputs(single_record):
    """A solved network's results come back keyed by attribute, long-form (§12)."""
    n = PyPSA.build(single_record.store)
    n.optimize(solver_name="highs")

    results = PyPSA.results(n)
    assert "p" in results
    assert "p_nom_opt" in results

    # Narwhals frames, so the seam names no one dataframe library, and lazy so a
    # tool may fetch on demand (§12).
    assert isinstance(results["p"], nw.LazyFrame)
    p = results["p"].collect()
    # The long schema's columns (§3), so the write path can persist it as-is.
    assert {"name", "snapshot", "scenario", "period", "value"} <= set(p.columns)
    # Keyed by attribute, so the type lives in the column - as `inputs/` does.
    assert "Generator" in set(p["component_type"].to_list())
    assert set(p["attribute"].to_list()) == {"p"}
    # Series output: one row per (name, snapshot), for the Generator rows.
    gen = p.filter(nw.col("component_type") == "Generator")
    assert len(gen) == len(n.snapshots) * len(n.c["Generator"].static)

    # A static output has no snapshot, and only the components whose value
    # differs from the default appear (some generators solve to p_nom_opt=0).
    nom = results["p_nom_opt"].collect()
    nom = nom.filter(nw.col("component_type") == "Generator")
    assert nom["snapshot"].is_null().all()
    nonzero = n.c["Generator"].static["p_nom_opt"] != 0.0
    assert set(nom["name"].to_list()) == set(n.c["Generator"].static.index[nonzero])


def test_results_concatenate_every_type_under_one_attribute(single_record):
    """One `p` frame holds every type's rows, matching `outputs/p.parquet` (§3.2)."""
    n = PyPSA.build(single_record.store)
    n.optimize(solver_name="highs")

    p = PyPSA.results(n)["p"].collect()
    types = set(p["component_type"].to_list())
    # `p` is a result of several types, so the concat is what is being tested;
    # keying by `(type, attribute)` would have split these into separate frames.
    assert len(types) > 1
    # Every row still says which type it belongs to, so nothing is lost.
    assert not p["component_type"].is_null().any()


def test_results_skips_outputs_still_at_their_default(single_record):
    """An unsolved network yields no `p`/`p_nom_opt` rows (§9.4 default rule)."""
    n = PyPSA.build(single_record.store)
    results = PyPSA.results(n)
    assert "p" not in results
    assert "p_nom_opt" not in results


def test_a_second_tool_needs_no_record_change(con, base_uri, ac_dc):
    """A tool is a plain object taking a record; adding one changes nothing here.

    No registration and no name dispatch: conformance to `Tool` is structural,
    so a second framework's module defines its own singleton and callers import
    it (§12).
    """
    from datarecord.tools.base import Tool

    class FakeTool:
        name = "fake"
        schema = Schema()

        def requires(self, store):
            return Requirements(dims=frozenset({"snapshot"}))

        def verify(self, store):
            return Requirements(component_types=frozenset({"Nope"}))

        def build(self, store):
            return "fake-model"

        def results(self, model):
            return {("Thing", "x"): model}

        def to_datarecord(self, model):
            return f"layer-of-{model}"

    fake = FakeTool()
    assert isinstance(fake, Tool)

    record = Revision.create(con)
    export_network(ac_dc, record, con)

    assert fake.requires(record.store).dims == {"snapshot"}
    assert fake.verify(record.store).component_types == {"Nope"}
    assert fake.build(record.store) == "fake-model"
    assert fake.results("m") == {("Thing", "x"): "m"}
    assert fake.to_datarecord("m") == "layer-of-m"
    # The record itself knows nothing of either tool.
    assert not hasattr(record, "to_model")
    assert not PyPSA.verify(record.store)


def test_schema_dims_stay_generic(con, base_uri, ac_dc):
    """`Dims` carries a dim PyPSA knows nothing about; axis names live in the tool.

    Declared but not `partial`, so the store's files need no column for it.
    Keying it would be a different matter, reported by the tool against the
    real store - see `test_verify_reports_unsupported_keys`.
    """
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    _with_schema(record, dims={**_DIMS, "vintage": "VARCHAR"})
    dims = record.node_cache.dims
    assert "vintage" in dims.schema.dims
    # No axis rows anywhere, so the dim is absent from the mapping rather than
    # present-and-empty (§4.2).
    assert "vintage" not in dims.axes
    # PyPSA's own required dims are still satisfied, and the extra dim is
    # simply not something the tool looks at.
    assert not PyPSA.verify(record.store)
    assert "vintage" in dims.schema.long_columns
