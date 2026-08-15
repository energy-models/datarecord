import pytest

from datarecord import duck


@pytest.fixture
def base_uri(tmp_path, monkeypatch):
    """Point layer_dir at a temporary directory for the whole test."""
    monkeypatch.setattr(duck, "DEFAULT_BASE_URI", str(tmp_path))
    return str(tmp_path)


@pytest.fixture
def con(base_uri):
    connection = duck.connect(base_uri=base_uri)
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def ac_dc():
    """PyPSA's `ac_dc_meshed`, with its loads renamed off their buses.

    The example names each `Load` after the `Bus` it sits on, which a record
    cannot represent: names are unique across component types,
    and `PyPSA.to_datarecord` rejects such a network rather than renaming it. Renaming here is the test suite standing in for the caller that has
    to reconcile the two vocabularies; `test_tools.py` pins the rejection
    itself.

    Notes
    -----
    - [name is unique across types](https://energy-models.github.io/datarecord/design/format/#name-is-unique-across-types)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    import pypsa

    from tests.fixtures import rename_components

    n = pypsa.examples.ac_dc_meshed()
    rename_components(n, "Load", " Load")
    return n
