"""
Fixtures et markers pour les tests GitHub.
Les tests marqués @pytest.mark.needs_aiohttp sont skippés si aiohttp n'est pas installé.
"""
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "needs_aiohttp: test requires aiohttp installed"
    )


@pytest.fixture(autouse=False)
def require_aiohttp():
    """Skip le test si aiohttp n'est pas disponible."""
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        pytest.skip("aiohttp not installed — run: pip install aiohttp")
