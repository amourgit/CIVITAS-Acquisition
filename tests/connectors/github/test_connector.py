"""
Tests du GitHubConnector avec client HTTP mocké.
Zéro appel réseau réel.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, AsyncMock
from civitas_acquisition.connectors.code_repos.github.connector import (
    GitHubConnector, _parse_composite_cursor, _make_composite_cursor,
)
from civitas_acquisition.connectors.code_repos.github.models import (
    GitHubRepo, GitHubIssue, GitHubRelease,
)
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType
from civitas_acquisition.contracts.models.cursor import Cursor
import json


def make_config(**options) -> ConnectorConfig:
    return ConnectorConfig(
        instance_id="inst-github-1",
        connector_id="github",
        credentials={"token": "ghp_test_token"},
        options={
            "repos": ["org/test-repo"],
            "resource_types": ["issues"],
            **options,
        },
    )


class TestConnectorManifest:
    def test_connector_id(self):
        c = GitHubConnector()
        assert c.manifest().connector_id == "github"

    def test_supporte_polling_webhook_manual(self):
        m = GitHubConnector().manifest()
        assert ChannelType.POLLING in m.supported_channels
        assert ChannelType.WEBHOOK in m.supported_channels
        assert ChannelType.MANUAL in m.supported_channels

    def test_supports_cursor_et_delta(self):
        m = GitHubConnector().manifest()
        assert m.supports_cursor is True
        assert m.supports_delta is True

    def test_credentials_requis(self):
        m = GitHubConnector().manifest()
        keys = [c.key for c in m.required_credentials]
        assert "token" in keys


class TestCursorComposite:
    def test_parse_cursor_none(self):
        assert _parse_composite_cursor(None) == {}

    def test_parse_cursor_valide(self):
        cursor = Cursor(
            value=json.dumps({"issues:org/repo": "2024-01-15T10:00:00Z"}),
            source_type="token", connector_id="github", instance_id="inst-1",
        )
        cursors = _parse_composite_cursor(cursor)
        assert cursors["issues:org/repo"] == "2024-01-15T10:00:00Z"

    def test_parse_cursor_invalide_retourne_vide(self):
        cursor = Cursor(value="not-json", source_type="token", connector_id="github", instance_id="inst-1")
        assert _parse_composite_cursor(cursor) == {}

    def test_make_composite_cursor(self):
        cursors = {"issues:org/repo": "2024-01-20T00:00:00Z", "commits:org/repo": "abc123"}
        cursor = _make_composite_cursor(cursors, "github", "inst-1")
        parsed = json.loads(cursor.value)
        assert parsed["issues:org/repo"] == "2024-01-20T00:00:00Z"


class TestConnectorPull:
    """Tests du pull avec fetcher complètement mocké."""

    @pytest.fixture
    def connector(self):
        return GitHubConnector()

    async def _connect_with_mock(self, connector, config):
        """Connect le connector avec un client mocké."""
        from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth
        from civitas_acquisition.connectors.code_repos.github.client import GitHubClient
        from civitas_acquisition.connectors.code_repos.github.fetcher import GitHubFetcher
        from civitas_acquisition.connectors.code_repos.github.mapper import GitHubMapper
        from civitas_acquisition.connectors.code_repos.github.webhook import GitHubWebhookParser

        connector._config = config
        connector._connected = True
        connector._auth = GitHubAuth.from_pat("ghp_test")
        connector._client = MagicMock(spec=GitHubClient)
        connector._mapper = GitHubMapper(instance_id=config.instance_id)
        connector._webhook_parser = GitHubWebhookParser()
        connector._repos = config.get_option("repos", [])
        connector._resource_types = config.get_option("resource_types", ["issues"])
        connector._default_branch = config.get_option("branch", "main")
        connector._file_patterns = config.get_option("file_patterns")
        connector._owner = config.get_option("owner")
        connector._is_org = config.get_option("is_org", False)
        return connector

    async def test_pull_issues_yield_raw_documents(self, connector):
        config = make_config(resource_types=["issues"])
        connector = await self._connect_with_mock(connector, config)

        # Mocker le fetcher
        mock_fetcher = MagicMock()

        async def fake_fetch_repo(full_name):
            return None

        async def fake_issues(repo, since=None, state="all"):
            yield GitHubIssue(
                number=1, title="Test issue", body="Issue body",
                state="open", html_url="https://github.com/org/test-repo/issues/1",
                created_at="2024-01-10T10:00:00Z", updated_at="2024-01-15T12:00:00Z",
                closed_at=None, labels=("bug",), assignees=(), author="alice",
                comments_count=0, milestone=None, repo_full_name="org/test-repo",
                comments=[],
            )

        mock_fetcher.fetch_repo = fake_fetch_repo
        mock_fetcher.fetch_issues = fake_issues
        connector._fetcher = mock_fetcher

        docs = []
        async for doc in connector._do_pull(cursor=None, batch_size=10):
            docs.append(doc)

        assert len(docs) == 1
        assert docs[0].source_ref.connector_id == "github"
        payload = json.loads(docs[0].content)
        assert payload["number"] == 1
        assert payload["title"] == "Test issue"

    async def test_pull_avec_cursor_since_transmis(self, connector):
        config = make_config(resource_types=["issues"])
        connector = await self._connect_with_mock(connector, config)

        received_since = []
        mock_fetcher = MagicMock()

        async def fake_fetch_repo(full_name): return None
        async def fake_issues(repo, since=None, state="all"):
            received_since.append(since)
            return
            yield

        mock_fetcher.fetch_repo = fake_fetch_repo
        mock_fetcher.fetch_issues = fake_issues
        connector._fetcher = mock_fetcher

        # Cursor avec since déjà défini
        cursor = _make_composite_cursor(
            {"issues:org/test-repo": "2024-01-10T00:00:00Z"},
            connector_id="github", instance_id="inst-github-1",
        )
        async for _ in connector._do_pull(cursor=cursor, batch_size=10):
            pass

        assert received_since[0] == "2024-01-10T00:00:00Z"

    async def test_batch_size_respecte(self, connector):
        config = make_config(resource_types=["issues"])
        connector = await self._connect_with_mock(connector, config)
        mock_fetcher = MagicMock()

        async def fake_fetch_repo(full_name): return None
        async def fake_issues(repo, since=None, state="all"):
            for i in range(20):   # génère 20 issues
                yield GitHubIssue(
                    number=i, title=f"Issue {i}", body="",
                    state="open", html_url=f"https://github.com/org/test-repo/issues/{i}",
                    created_at="2024-01-01T00:00:00Z", updated_at=f"2024-01-{i+1:02d}T00:00:00Z",
                    closed_at=None, labels=(), assignees=(), author="bot",
                    comments_count=0, milestone=None, repo_full_name="org/test-repo",
                    comments=[],
                )

        mock_fetcher.fetch_repo = fake_fetch_repo
        mock_fetcher.fetch_issues = fake_issues
        connector._fetcher = mock_fetcher

        docs = []
        async for doc in connector._do_pull(cursor=None, batch_size=5):
            docs.append(doc)

        assert len(docs) == 5   # limité par batch_size


class TestHealthcheck:

    async def test_healthcheck_ok(self):
        connector = GitHubConnector()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"rate": {"remaining": 4500, "limit": 5000}})
        connector._client = mock_client
        connector._connected = True

        status = await connector.healthcheck()
        assert status.healthy is True
        assert status.detail.get("rate_remaining") == 4500

    async def test_healthcheck_fail(self):
        connector = GitHubConnector()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Connection refused"))
        connector._client = mock_client
        connector._connected = True

        status = await connector.healthcheck()
        assert status.healthy is False
        assert "Connection refused" in status.error
