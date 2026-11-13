import pytest

from src.lakehouse.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    """Start Spark once for the whole test run (it takes ~10s), then stop it."""
    s = get_spark("tests")
    yield s
    s.stop()