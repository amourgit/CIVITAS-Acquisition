"""Guard aiohttp pour tests Notion."""
import pytest

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


@pytest.fixture(autouse=True)
def skip_if_no_aiohttp():
    if not HAS_AIOHTTP:
        pytest.skip("aiohttp not installed — run: pip install aiohttp")
