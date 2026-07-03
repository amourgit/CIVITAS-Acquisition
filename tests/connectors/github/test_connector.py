"""Tests du GitHubConnector — avec guard aiohttp."""
import pytest
import json

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from civitas_acquisition.connectors.code_repos.github.connector import (
    GitHubConnector, _parse_composite_cursor, _make_composite_cursor,
)
from civitas_acquisition.connectors.code_repos.github.models import GitHubIssue
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType
from civitas_acquisition.contracts.models.cursor import Cursor


def make_config(**options) -> ConnectorConfig:
    return ConnectorConfig(
        instance_id="inst-github-1", connector_id="github",
        credentials={"token": "ghp_test_token"},
        options={"repos": ["org/test-repo"], "resource_types": ["issues"], **options},
    )


class TestConnectorManifest:
    def test_connector_id(self):
        assert GitHubConnector().manifest().connector_id == "github"

    def test_version_v2(self):
        assert GitHubConnector().manifest().version == "2.0.0"

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
        keys = [c.key for c in GitHubConnector().manifest().required_credentials]
        assert "token" in keys


class TestCursorComposite:
    def test_parse_cursor_none(self):
        assert _parse_composite_cursor(None) == {}

    def test_parse_cursor_valide(self):
        cursor = Cursor(
            value=json.dumps({"issues:org/repo": "2024-01-15T10:00:00Z"}),
            source_type="token", connector_id="github", instance_id="inst-1",
        )
        assert _parse_composite_cursor(cursor)["issues:org/repo"] == "2024-01-15T10:00:00Z"

    def test_parse_cursor_invalide_retourne_vide(self):
        cursor = Cursor(value="not-json", source_type="token", connector_id="github", instance_id="inst-1")
        assert _parse_composite_cursor(cursor) == {}

    def test_make_composite_cursor(self):
        cursors = {"issues:org/repo": "2024-01-20T00:00:00Z"}
        cursor = _make_composite_cursor(cursors, "github", "inst-1")
        assert json.loads(cursor.value)["issues:org/repo"] == "2024-01-20T00:00:00Z"


class TestConnectorPull:
    async def _setup_connector(self, options=None):
        from unittest.mock import AsyncMock, MagicMock
        from civitas_acquisition.connectors.code_repos.github.mapper import GitHubMapper
        from civitas_acquisition.connectors.code_repos.github.webhook import GitHubWebhookParser
        from civitas_acquisition.connectors.code_repos.github.webhook_manager import (
            GitHubWebhookManager, WebhookRegistry,
        )

        config = make_config(**(options or {}))
        connector = GitHubConnector()
        connector._config = config
        connector._connected = True
        connector._auth = MagicMock()
        connector._auth.is_app_auth = False
        connector._client = MagicMock()
        connector._mapper = GitHubMapper(instance_id=config.instance_id)
        connector._webhook_parser = GitHubWebhookParser()
        connector._webhook_manager = GitHubWebhookManager(
            client=connector._client,
            registry=WebhookRegistry(storage_path=None),
            instance_id=config.instance_id,
        )
        connector._repos = config.get_option("repos", [])
        connector._resource_types = config.get_option("resource_types", ["issues"])
        connector._default_branch = "main"
        connector._file_patterns = None
        connector._owner = None
        connector._is_org = False
        connector._auto_webhook = False
        connector._webhook_url = ""
        connector._webhook_events = []
        return connector

    async def test_pull_issues_yield_raw_documents(self):
        connector = await self._setup_connector()
        mock_fetcher = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()

        async def fake_fetch_repo(full_name): return None
        async def fake_issues(repo, since=None, state="all"):
            yield GitHubIssue(
                number=1, title="Test issue", body="Issue body",
                state="open", html_url="https://github.com/org/test-repo/issues/1",
                created_at="2024-01-10T10:00:00Z", updated_at="2024-01-15T12:00:00Z",
                closed_at=None, labels=("bug",), assignees=(), author="alice",
                comments_count=0, milestone=None, repo_full_name="org/test-repo", comments=[],
            )

        mock_fetcher.fetch_repo = fake_fetch_repo
        mock_fetcher.fetch_issues = fake_issues
        connector._fetcher = mock_fetcher

        docs = []
        async for doc in connector._do_pull(cursor=None, batch_size=10):
            docs.append(doc)

        assert len(docs) == 1
        payload = json.loads(docs[0].content)
        assert payload["number"] == 1

    async def test_batch_size_respecte(self):
        connector = await self._setup_connector()
        mock_fetcher = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()

        async def fake_fetch_repo(full_name): return None
        async def fake_issues(repo, since=None, state="all"):
            for i in range(20):
                yield GitHubIssue(
                    number=i, title=f"Issue {i}", body="",
                    state="open", html_url=f"https://github.com/org/test-repo/issues/{i}",
                    created_at="2024-01-01T00:00:00Z", updated_at=f"2024-01-{i+1:02d}T00:00:00Z",
                    closed_at=None, labels=(), assignees=(), author="bot",
                    comments_count=0, milestone=None, repo_full_name="org/test-repo", comments=[],
                )

        mock_fetcher.fetch_repo = fake_fetch_repo
        mock_fetcher.fetch_issues = fake_issues
        connector._fetcher = mock_fetcher

        docs = []
        async for doc in connector._do_pull(cursor=None, batch_size=5):
            docs.append(doc)
        assert len(docs) == 5


class TestHealthcheck:
    async def test_healthcheck_ok(self):
        from unittest.mock import AsyncMock
        connector = GitHubConnector()
        connector._client = AsyncMock()
        connector._client.get = AsyncMock(return_value={"rate": {"remaining": 4500, "limit": 5000}})
        connector._connected = True
        status = await connector.healthcheck()
        assert status.healthy is True
        assert status.detail.get("rate_remaining") == 4500

    async def test_healthcheck_fail(self):
        from unittest.mock import AsyncMock
        connector = GitHubConnector()
        connector._client = AsyncMock()
        connector._client.get = AsyncMock(side_effect=Exception("Connection refused"))
        connector._connected = True
        status = await connector.healthcheck()
        assert status.healthy is False
