"""Tests pour GitHubClient GraphQL et write methods — nécessite aiohttp."""
import pytest

pytestmark = pytest.mark.usefixtures("require_aiohttp")

try:
    import aiohttp
    from civitas_acquisition.connectors.code_repos.github.client import GitHubClient
    from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth
    from civitas_acquisition.contracts.errors.connector_errors import (
        ConnectorFatalError,
    )
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")


def make_client():
    from civitas_acquisition.connectors.code_repos.github.client import GitHubClient
    from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth
    return GitHubClient(auth=GitHubAuth.from_pat("ghp_test"), timeout_s=5.0)


class TestGraphQL:
    async def test_graphql_succes(self):
        from unittest.mock import AsyncMock, MagicMock
        client = make_client()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {}
        mock_resp.json = AsyncMock(return_value={"data": {"viewer": {"login": "alice"}}})
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=MagicMock(
            __aenter__=AsyncMock(return_value=mock_resp),
            __aexit__=AsyncMock(return_value=None),
        ))
        client._session = mock_session
        result = await client.graphql("query { viewer { login } }")
        assert result["viewer"]["login"] == "alice"

    async def test_graphql_errors_leve_fatal(self):
        from unittest.mock import AsyncMock, MagicMock
        from civitas_acquisition.contracts.errors.connector_errors import ConnectorFatalError
        client = make_client()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.headers = {}
        mock_resp.json = AsyncMock(return_value={
            "errors": [{"message": "Field 'xyz' doesn't exist"}],
            "data": None,
        })
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=MagicMock(
            __aenter__=AsyncMock(return_value=mock_resp),
            __aexit__=AsyncMock(return_value=None),
        ))
        client._session = mock_session
        with pytest.raises(ConnectorFatalError, match="GraphQL errors"):
            await client.graphql("query { xyz }")


class TestWriteOperations:
    def _make_client_with_mock(self, status: int, body: dict):
        from unittest.mock import AsyncMock, MagicMock
        client = make_client()
        mock_resp = AsyncMock()
        mock_resp.status = status
        mock_resp.headers = {}
        mock_resp.json = AsyncMock(return_value=body)
        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=MagicMock(
            __aenter__=AsyncMock(return_value=mock_resp),
            __aexit__=AsyncMock(return_value=None),
        ))
        client._session = mock_session
        return client

    async def test_post_201_retourne_body(self):
        client = self._make_client_with_mock(201, {"id": 42})
        result = await client.post("/repos/org/repo/issues", body={"title": "Test"})
        assert result["id"] == 42

    async def test_delete_204_retourne_none(self):
        client = self._make_client_with_mock(204, {})
        result = await client.delete("/repos/org/repo/hooks/99")
        assert result is None
