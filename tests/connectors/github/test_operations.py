"""Tests pour GitHubOperations — write operations."""
import pytest
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
        mock_client.post.return_value = {"number": 1, "html_url": "https://github.com/org/repo/issues/1"}
        result = await ops.create_issue("org/repo", "Bug: crash", body="Steps...", labels=["bug"])
        mock_client.post.assert_called_once()
        path, = mock_client.post.call_args[0]
        assert "/repos/org/repo/issues" in path
        body = mock_client.post.call_args[1]["body"]
        assert body["title"] == "Bug: crash"
        assert "bug" in body["labels"]
        assert result["number"] == 1

    async def test_sans_body(self, ops, mock_client):
        mock_client.post.return_value = {"number": 2}
        await ops.create_issue("org/repo", "Simple issue")
        body = mock_client.post.call_args[1]["body"]
        assert "body" not in body


class TestUpdateIssue:
    async def test_update_state(self, ops, mock_client):
        mock_client.patch.return_value = {"number": 5, "state": "closed"}
        result = await ops.update_issue("org/repo", 5, state="closed", state_reason="completed")
        mock_client.patch.assert_called_once()
        body = mock_client.patch.call_args[1]["body"]
        assert body["state"] == "closed"
        assert body["state_reason"] == "completed"

    async def test_sans_champ_leve_erreur(self, ops, mock_client):
        with pytest.raises(ConnectorFatalError):
            await ops.update_issue("org/repo", 5)


class TestCreateBranch:
    async def test_appel_correct(self, ops, mock_client):
        mock_client.post.return_value = {"ref": "refs/heads/feat/dark-mode"}
        await ops.create_branch("org/repo", "feat/dark-mode", from_sha="abc123")
        body = mock_client.post.call_args[1]["body"]
        assert body["ref"] == "refs/heads/feat/dark-mode"
        assert body["sha"] == "abc123"

    async def test_find_branch_none_si_404(self, ops, mock_client):
        from civitas_acquisition.connectors.code_repos.github.client import ResourceNotFoundError
        mock_client.get = AsyncMock(side_effect=ResourceNotFoundError("/repos/org/repo/branches/x"))
        ops._client = mock_client
        result = await ops.find_branch("org/repo", "nonexistent")
        assert result is None


class TestCreatePR:
    async def test_creer_pr(self, ops, mock_client):
        mock_client.post.return_value = {"number": 10, "html_url": "https://github.com/org/repo/pull/10"}
        result = await ops.create_pull_request(
            "org/repo", title="feat: dark mode", head="feat/dark-mode", base="main", draft=False
        )
        body = mock_client.post.call_args[1]["body"]
        assert body["head"] == "feat/dark-mode"
        assert body["base"] == "main"
        assert result["number"] == 10


class TestGraphQL:
    async def test_raw_graphql(self, ops, mock_client):
        mock_client.graphql.return_value = {"viewer": {"login": "alice"}}
        result = await ops.raw_graphql("query { viewer { login } }")
        assert result["viewer"]["login"] == "alice"

    async def test_get_discussions(self, ops, mock_client):
        mock_client.graphql.return_value = {
            "repository": {
                "discussions": {
                    "nodes": [
                        {"id": "disc-1", "title": "How to use X?", "body": "Details...",
                         "url": "https://github.com/org/repo/discussions/1",
                         "author": {"login": "bob"}, "createdAt": "2024-01-10T00:00:00Z",
                         "updatedAt": "2024-01-15T00:00:00Z", "category": {"name": "Q&A"},
                         "comments": {"nodes": []}}
                    ]
                }
            }
        }
        discussions = await ops.get_discussions("org", "repo")
        assert len(discussions) == 1
        assert discussions[0]["title"] == "How to use X?"

    async def test_create_discussion_comment(self, ops, mock_client):
        mock_client.graphql.return_value = {
            "addDiscussionComment": {
                "comment": {"id": "c-1", "body": "Great question!", "author": {"login": "alice"}, "createdAt": "2024-01-16T00:00:00Z"}
            }
        }
        comment = await ops.create_discussion_comment("disc-1", "Great question!")
        assert comment["body"] == "Great question!"


class TestParseRepo:
    def test_valide(self):
        assert _parse_repo("owner/repo") == ("owner", "repo")

    def test_invalide_leve_erreur(self):
        with pytest.raises(ConnectorFatalError):
            _parse_repo("invalid-format")

    def test_org_avec_slash_dans_nom(self):
        owner, repo = _parse_repo("my-org/my-repo-name")
        assert owner == "my-org"
        assert repo == "my-repo-name"
