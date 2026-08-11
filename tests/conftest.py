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
    import pypsa

    return pypsa.examples.ac_dc_meshed()
