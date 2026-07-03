"""Tests pour GitHubOperations — write operations."""
import pytest

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from unittest.mock import AsyncMock
from civitas_acquisition.connectors.code_repos.github.operations import (
    GitHubOperations, _parse_repo,
)
from civitas_acquisition.contracts.errors.connector_errors import ConnectorFatalError


@pytest.fixture
def mock_client():
    c = AsyncMock()
    c.post   = AsyncMock()
    c.patch  = AsyncMock()
    c.delete = AsyncMock(return_value=None)
    c.graphql = AsyncMock()
    return c


@pytest.fixture
def ops(mock_client):
    return GitHubOperations(mock_client)


class TestCreateIssue:
    async def test_appel_correct(self, ops, mock_client):
        mock_client.post.return_value = {"number": 1}
        await ops.create_issue("org/repo", "Bug", body="Steps", labels=["bug"])
        body = mock_client.post.call_args[1]["body"]
        assert body["title"] == "Bug"
        assert "bug" in body["labels"]

    async def test_sans_body(self, ops, mock_client):
        mock_client.post.return_value = {"number": 2}
        await ops.create_issue("org/repo", "Simple")
        body = mock_client.post.call_args[1]["body"]
        assert "body" not in body


class TestUpdateIssue:
    async def test_update_state(self, ops, mock_client):
        mock_client.patch.return_value = {"number": 5, "state": "closed"}
        await ops.update_issue("org/repo", 5, state="closed", state_reason="completed")
        body = mock_client.patch.call_args[1]["body"]
        assert body["state"] == "closed"

    async def test_sans_champ_leve_erreur(self, ops, mock_client):
        with pytest.raises(ConnectorFatalError):
            await ops.update_issue("org/repo", 5)


class TestCreateBranch:
    async def test_appel_correct(self, ops, mock_client):
        mock_client.post.return_value = {"ref": "refs/heads/feat/x"}
        await ops.create_branch("org/repo", "feat/x", from_sha="abc123")
        body = mock_client.post.call_args[1]["body"]
        assert body["ref"] == "refs/heads/feat/x"
        assert body["sha"] == "abc123"

    async def test_find_branch_none_si_404(self, ops, mock_client):
        from civitas_acquisition.connectors.code_repos.github.client import ResourceNotFoundError
        ops._client.get = AsyncMock(side_effect=ResourceNotFoundError("/repos/org/repo/branches/x"))
        result = await ops.find_branch("org/repo", "nonexistent")
        assert result is None


class TestGraphQL:
    async def test_raw_graphql(self, ops, mock_client):
        mock_client.graphql.return_value = {"viewer": {"login": "alice"}}
        result = await ops.raw_graphql("query { viewer { login } }")
        assert result["viewer"]["login"] == "alice"

    async def test_get_discussions(self, ops, mock_client):
        mock_client.graphql.return_value = {
            "repository": {"discussions": {"nodes": [
                {"id": "disc-1", "title": "How to use X?", "body": "Details...",
                 "url": "https://github.com/org/repo/discussions/1",
                 "author": {"login": "bob"}, "createdAt": "2024-01-10T00:00:00Z",
                 "updatedAt": "2024-01-15T00:00:00Z", "category": {"name": "Q&A"},
                 "comments": {"nodes": []}}
            ]}}
        }
        discussions = await ops.get_discussions("org", "repo")
        assert len(discussions) == 1
        assert discussions[0]["title"] == "How to use X?"


class TestParseRepo:
    def test_valide(self):
        assert _parse_repo("owner/repo") == ("owner", "repo")

    def test_invalide_leve_erreur(self):
        with pytest.raises(ConnectorFatalError):
            _parse_repo("invalid-format")
