"""Tests pour GitHubWebhookManager — cycle de vie complet des webhooks."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from civitas_acquisition.connectors.code_repos.github.webhook_manager import (
    GitHubWebhookManager,
    WebhookRegistry,
    RegisteredWebhook,
    DEFAULT_EVENTS,
    SUPPORTED_EVENTS,
)
from civitas_acquisition.connectors.code_repos.github.client import ResourceNotFoundError


INSTANCE_ID  = "inst-github-1"
REPO         = "org/my-repo"
WEBHOOK_URL  = "https://civitas.example.com/webhooks/github"


@pytest.fixture
def registry():
    return WebhookRegistry(storage_path=None)   # in-memory


@pytest.fixture
def mock_client():
    client = AsyncMock()
    client.post   = AsyncMock(return_value={"id": 42, "active": True, "events": DEFAULT_EVENTS, "config": {"url": WEBHOOK_URL}, "created_at": "2024-01-15T10:00:00Z"})
    client.delete = AsyncMock(return_value=None)
    client.patch  = AsyncMock(return_value={"id": 42})
    client.collect_all = AsyncMock(return_value=[])
    return client


@pytest.fixture
def manager(mock_client, registry):
    return GitHubWebhookManager(
        client=mock_client,
        registry=registry,
        instance_id=INSTANCE_ID,
    )


class TestRegister:

    async def test_register_cree_webhook_github(self, manager, mock_client):
        hook = await manager.register(
            repo_full_name=REPO,
            webhook_url=WEBHOOK_URL,
            events=["push", "issues"],
        )
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert f"/repos/org/my-repo/hooks" in call_args[0][0]
        assert hook.webhook_id == 42
        assert hook.repo_full_name == REPO

    async def test_register_stocke_dans_registry(self, manager, registry):
        await manager.register(repo_full_name=REPO, webhook_url=WEBHOOK_URL)
        stored = registry.get(INSTANCE_ID, REPO)
        assert stored is not None
        assert stored.webhook_id == 42

    async def test_register_idempotent_si_existant(self, manager, registry, mock_client):
        """Si le webhook existe déjà en registry, pas de nouvel appel API."""
        registry.put(RegisteredWebhook(
            webhook_id=99, repo_full_name=REPO,
            events=DEFAULT_EVENTS, webhook_url=WEBHOOK_URL,
            instance_id=INSTANCE_ID, registered_at="2024-01-01T00:00:00Z",
        ))
        hook = await manager.register(repo_full_name=REPO, webhook_url=WEBHOOK_URL)
        mock_client.post.assert_not_called()
        assert hook.webhook_id == 99

    async def test_register_dedup_via_github_api(self, manager, mock_client):
        """Si pas en registry mais trouvé via API, retourne l'existant."""
        mock_client.collect_all = AsyncMock(return_value=[
            {"id": 77, "active": True, "events": DEFAULT_EVENTS,
             "config": {"url": WEBHOOK_URL}, "created_at": "2024-01-10T00:00:00Z"}
        ])
        hook = await manager.register(repo_full_name=REPO, webhook_url=WEBHOOK_URL)
        mock_client.post.assert_not_called()
        assert hook.webhook_id == 77

    async def test_register_avec_secret(self, manager, mock_client):
        await manager.register(
            repo_full_name=REPO, webhook_url=WEBHOOK_URL, secret="mysecret"
        )
        body = mock_client.post.call_args[1]["body"]
        assert body["config"]["secret"] == "mysecret"

    async def test_register_format_repo_invalide(self, manager):
        with pytest.raises(Exception):
            await manager.register(repo_full_name="invalid-no-slash", webhook_url=WEBHOOK_URL)


class TestUnregister:

    async def test_unregister_supprime_github(self, manager, registry, mock_client):
        registry.put(RegisteredWebhook(
            webhook_id=42, repo_full_name=REPO, events=DEFAULT_EVENTS,
            webhook_url=WEBHOOK_URL, instance_id=INSTANCE_ID,
            registered_at="2024-01-01T00:00:00Z",
        ))
        result = await manager.unregister(REPO)
        assert result is True
        mock_client.delete.assert_called_once()
        assert "/hooks/42" in mock_client.delete.call_args[0][0]

    async def test_unregister_retire_du_registry(self, manager, registry):
        registry.put(RegisteredWebhook(
            webhook_id=42, repo_full_name=REPO, events=DEFAULT_EVENTS,
            webhook_url=WEBHOOK_URL, instance_id=INSTANCE_ID,
            registered_at="2024-01-01T00:00:00Z",
        ))
        await manager.unregister(REPO)
        assert registry.get(INSTANCE_ID, REPO) is None

    async def test_unregister_gracieux_si_404(self, manager, registry, mock_client):
        """404 GitHub est géré — le registry est nettoyé quand même."""
        registry.put(RegisteredWebhook(
            webhook_id=99, repo_full_name=REPO, events=DEFAULT_EVENTS,
            webhook_url=WEBHOOK_URL, instance_id=INSTANCE_ID,
            registered_at="2024-01-01T00:00:00Z",
        ))
        mock_client.delete = AsyncMock(side_effect=ResourceNotFoundError("/repos/org/my-repo/hooks/99"))
        result = await manager.unregister(REPO)
        assert result is True
        assert registry.get(INSTANCE_ID, REPO) is None

    async def test_unregister_sans_registry_retourne_false(self, manager):
        result = await manager.unregister("org/nonexistent")
        assert result is False

    async def test_unregister_avec_id_explicite(self, manager, mock_client):
        result = await manager.unregister(REPO, webhook_id=55)
        assert result is True
        assert "/hooks/55" in mock_client.delete.call_args[0][0]


class TestUpdateEvents:

    async def test_update_events(self, manager, registry, mock_client):
        registry.put(RegisteredWebhook(
            webhook_id=42, repo_full_name=REPO, events=["push"],
            webhook_url=WEBHOOK_URL, instance_id=INSTANCE_ID,
            registered_at="2024-01-01T00:00:00Z",
        ))
        result = await manager.update_events(REPO, ["push", "issues", "release"])
        assert result is True
        mock_client.patch.assert_called_once()
        body = mock_client.patch.call_args[1]["body"]
        assert "release" in body["events"]

    async def test_update_events_sans_registry_retourne_false(self, manager):
        result = await manager.update_events("org/nope", ["push"])
        assert result is False


class TestUnregisterAll:

    async def test_unregister_all(self, manager, registry, mock_client):
        for repo in ["org/repo1", "org/repo2"]:
            registry.put(RegisteredWebhook(
                webhook_id=10 + len(repo), repo_full_name=repo,
                events=DEFAULT_EVENTS, webhook_url=WEBHOOK_URL,
                instance_id=INSTANCE_ID, registered_at="2024-01-01T00:00:00Z",
            ))
        count = await manager.unregister_all()
        assert count == 2


class TestWebhookRegistry:

    def test_put_et_get(self, registry):
        hook = RegisteredWebhook(
            webhook_id=1, repo_full_name="org/repo", events=["push"],
            webhook_url="https://example.com", instance_id="inst-1",
            registered_at="2024-01-01T00:00:00Z",
        )
        registry.put(hook)
        stored = registry.get("inst-1", "org/repo")
        assert stored is not None
        assert stored.webhook_id == 1

    def test_remove(self, registry):
        hook = RegisteredWebhook(
            webhook_id=1, repo_full_name="org/repo", events=["push"],
            webhook_url="https://example.com", instance_id="inst-1",
            registered_at="2024-01-01T00:00:00Z",
        )
        registry.put(hook)
        registry.remove("inst-1", "org/repo")
        assert registry.get("inst-1", "org/repo") is None

    def test_list_all(self, registry):
        for i in range(3):
            registry.put(RegisteredWebhook(
                webhook_id=i, repo_full_name=f"org/repo{i}", events=["push"],
                webhook_url="https://example.com", instance_id="inst-1",
                registered_at="2024-01-01T00:00:00Z",
            ))
        assert len(registry.list_all()) == 3

    def test_persistence_fichier(self, tmp_path):
        path = str(tmp_path / "webhooks.json")
        r1 = WebhookRegistry(storage_path=path)
        r1.put(RegisteredWebhook(
            webhook_id=99, repo_full_name="org/repo", events=["push"],
            webhook_url="https://civitas.example.com", instance_id="inst-1",
            registered_at="2024-01-01T00:00:00Z",
        ))
        # Recharger depuis fichier
        r2 = WebhookRegistry(storage_path=path)
        stored = r2.get("inst-1", "org/repo")
        assert stored is not None
        assert stored.webhook_id == 99
