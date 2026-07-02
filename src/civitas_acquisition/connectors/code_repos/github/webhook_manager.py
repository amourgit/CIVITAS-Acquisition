"""
GitHubWebhookManager — gestion automatique du cycle de vie des webhooks GitHub.

C'est le gap le plus critique identifié dans l'analyse Activepieces.
Activepieces crée et supprime les webhooks via l'API GitHub automatiquement.
CIVITAS doit faire la même chose.

Cycle de vie complet :
  register()    → POST /repos/{owner}/{repo}/hooks (crée le webhook)
  unregister()  → DELETE /repos/{owner}/{repo}/hooks/{id} (supprime)
  list_hooks()  → GET  /repos/{owner}/{repo}/hooks (audit)
  find_existing() → vérifie avant création pour éviter les doublons

Stockage des IDs : WebhookRegistry (JSON file ou in-memory)
Pattern Activepieces : onEnable/onDisable avec store.put/store.get

Events GitHub supportés :
  push, issues, issue_comment, pull_request, pull_request_review,
  release, create, delete, repository, star, fork, discussion,
  discussion_comment, check_run, workflow_run
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from civitas_acquisition.connectors.code_repos.github.client import (
    GitHubClient,
    ResourceNotFoundError,
)
from civitas_acquisition.contracts.errors.connector_errors import ConnectorFatalError

logger = logging.getLogger(__name__)

# Tous les event types GitHub supportés par CIVITAS
SUPPORTED_EVENTS: frozenset[str] = frozenset([
    "push",
    "issues",
    "issue_comment",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "release",
    "create",
    "delete",
    "repository",
    "star",
    "fork",
    "discussion",
    "discussion_comment",
    "check_run",
    "workflow_run",
    "commit_comment",
    "member",
    "milestone",
])

# Events par défaut si non spécifiés
DEFAULT_EVENTS: list[str] = [
    "push",
    "issues",
    "pull_request",
    "release",
]


@dataclass
class RegisteredWebhook:
    """Un webhook enregistré sur GitHub."""
    webhook_id: int
    repo_full_name: str          # "owner/repo"
    events: list[str]
    webhook_url: str
    instance_id: str
    registered_at: str           # ISO-8601
    active: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> RegisteredWebhook:
        return cls(**data)


class WebhookRegistry:
    """
    Stockage persistant des webhooks enregistrés.
    Évite les doublons et permet le cleanup propre.

    Implémentation par défaut : fichier JSON par instance.
    En production : PostgresWebhookRegistry.
    """

    def __init__(self, storage_path: Optional[str] = None) -> None:
        if storage_path:
            self._path: Optional[Path] = Path(storage_path)
            self._path.parent.mkdir(parents=True, exist_ok=True)
        else:
            self._path = None
        self._data: dict[str, RegisteredWebhook] = {}
        self._load()

    def _load(self) -> None:
        if self._path and self._path.exists():
            try:
                raw = json.loads(self._path.read_text())
                self._data = {
                    k: RegisteredWebhook.from_dict(v)
                    for k, v in raw.items()
                }
            except Exception as exc:
                logger.warning("Failed to load webhook registry: %s", exc)

    def _save(self) -> None:
        if not self._path:
            return
        try:
            self._path.write_text(
                json.dumps(
                    {k: v.to_dict() for k, v in self._data.items()},
                    indent=2,
                )
            )
        except Exception as exc:
            logger.warning("Failed to save webhook registry: %s", exc)

    def _key(self, instance_id: str, repo: str) -> str:
        return f"{instance_id}:{repo}"

    def put(self, hook: RegisteredWebhook) -> None:
        key = self._key(hook.instance_id, hook.repo_full_name)
        self._data[key] = hook
        self._save()

    def get(self, instance_id: str, repo: str) -> Optional[RegisteredWebhook]:
        return self._data.get(self._key(instance_id, repo))

    def remove(self, instance_id: str, repo: str) -> None:
        key = self._key(instance_id, repo)
        self._data.pop(key, None)
        self._save()

    def list_all(self) -> list[RegisteredWebhook]:
        return list(self._data.values())


class GitHubWebhookManager:
    """
    Gère le cycle de vie complet des webhooks GitHub.

    onEnable → register()  : crée le webhook, stocke l'ID
    onDisable → unregister() : supprime le webhook par ID stocké

    Garanties :
    - Idempotence : find_existing() évite les doublons
    - Cleanup propre : vérifie l'existence avant DELETE
    - Audit : registry persistant
    """

    def __init__(
        self,
        client: GitHubClient,
        registry: Optional[WebhookRegistry] = None,
        instance_id: str = "",
    ) -> None:
        self._client      = client
        self._registry    = registry or WebhookRegistry()
        self._instance_id = instance_id

    async def register(
        self,
        repo_full_name: str,         # "owner/repo"
        webhook_url: str,            # URL CIVITAS à notifier
        events: list[str] | None = None,
        secret: Optional[str] = None,
        insecure_ssl: bool = False,
    ) -> RegisteredWebhook:
        """
        Crée un webhook GitHub sur le repository.
        Si un webhook pour cette URL existe déjà, retourne l'existant.

        Pattern Activepieces : onEnable → POST /repos/{owner}/{repo}/hooks
        """
        owner, repo = self._parse_repo(repo_full_name)
        events_list  = events or DEFAULT_EVENTS
        self._validate_events(events_list)

        # Vérifier l'idempotence : existe-t-il déjà ?
        existing = await self.find_existing(repo_full_name, webhook_url)
        if existing:
            logger.info(
                "Webhook already exists for %s (id=%d) — reusing",
                repo_full_name, existing.webhook_id,
            )
            return existing

        body: dict = {
            "name":   "web",
            "active": True,
            "events": events_list,
            "config": {
                "url":          webhook_url,
                "content_type": "json",
                "insecure_ssl": "1" if insecure_ssl else "0",
            },
        }
        if secret:
            body["config"]["secret"] = secret

        data = await self._client.post(
            f"/repos/{owner}/{repo}/hooks",
            body=body,
        )

        hook = RegisteredWebhook(
            webhook_id=data["id"],
            repo_full_name=repo_full_name,
            events=events_list,
            webhook_url=webhook_url,
            instance_id=self._instance_id,
            registered_at=datetime.now(tz=timezone.utc).isoformat(),
            active=True,
        )
        self._registry.put(hook)

        logger.info(
            "Webhook registered: id=%d repo=%s events=%s",
            hook.webhook_id, repo_full_name, events_list,
        )
        return hook

    async def unregister(
        self,
        repo_full_name: str,
        webhook_id: Optional[int] = None,
    ) -> bool:
        """
        Supprime un webhook GitHub.
        Utilise l'ID stocké dans le registry si webhook_id n'est pas fourni.
        Retourne True si supprimé, False si non trouvé.

        Pattern Activepieces : onDisable → DELETE /repos/{owner}/{repo}/hooks/{id}
        """
        owner, repo = self._parse_repo(repo_full_name)

        # Récupérer l'ID depuis le registry si non fourni
        if webhook_id is None:
            stored = self._registry.get(self._instance_id, repo_full_name)
            if not stored:
                logger.warning(
                    "No stored webhook for %s (instance=%s)",
                    repo_full_name, self._instance_id,
                )
                return False
            webhook_id = stored.webhook_id

        try:
            await self._client.delete(f"/repos/{owner}/{repo}/hooks/{webhook_id}")
            self._registry.remove(self._instance_id, repo_full_name)
            logger.info(
                "Webhook unregistered: id=%d repo=%s",
                webhook_id, repo_full_name,
            )
            return True
        except ResourceNotFoundError:
            # Webhook déjà supprimé côté GitHub — nettoyer le registry quand même
            self._registry.remove(self._instance_id, repo_full_name)
            logger.info(
                "Webhook %d not found on GitHub (already deleted) — registry cleaned",
                webhook_id,
            )
            return True
        except Exception as exc:
            logger.error(
                "Failed to unregister webhook %d: %s", webhook_id, exc
            )
            return False

    async def find_existing(
        self,
        repo_full_name: str,
        webhook_url: str,
    ) -> Optional[RegisteredWebhook]:
        """
        Cherche un webhook existant pour cette URL sur GitHub.
        Interroge d'abord le registry local, puis l'API si pas trouvé.
        """
        # 1. Chercher dans le registry local
        stored = self._registry.get(self._instance_id, repo_full_name)
        if stored and stored.webhook_url == webhook_url:
            return stored

        # 2. Interroger l'API GitHub
        owner, repo = self._parse_repo(repo_full_name)
        try:
            hooks = await self._list_github_hooks(owner, repo)
            for hook in hooks:
                config = hook.get("config", {})
                if config.get("url") == webhook_url:
                    found = RegisteredWebhook(
                        webhook_id=hook["id"],
                        repo_full_name=repo_full_name,
                        events=hook.get("events", []),
                        webhook_url=webhook_url,
                        instance_id=self._instance_id,
                        registered_at=hook.get("created_at", ""),
                        active=hook.get("active", True),
                    )
                    self._registry.put(found)
                    return found
        except Exception as exc:
            logger.debug("Could not list hooks for %s: %s", repo_full_name, exc)

        return None

    async def list_hooks(self, repo_full_name: str) -> list[dict]:
        """Liste tous les webhooks d'un repository (audit)."""
        owner, repo = self._parse_repo(repo_full_name)
        return await self._list_github_hooks(owner, repo)

    async def update_events(
        self,
        repo_full_name: str,
        events: list[str],
    ) -> bool:
        """Met à jour les événements surveillés par le webhook."""
        stored = self._registry.get(self._instance_id, repo_full_name)
        if not stored:
            logger.warning("No stored webhook for %s", repo_full_name)
            return False

        owner, repo = self._parse_repo(repo_full_name)
        self._validate_events(events)

        await self._client.patch(
            f"/repos/{owner}/{repo}/hooks/{stored.webhook_id}",
            body={"events": events, "active": True},
        )

        updated = RegisteredWebhook(
            webhook_id=stored.webhook_id,
            repo_full_name=stored.repo_full_name,
            events=events,
            webhook_url=stored.webhook_url,
            instance_id=stored.instance_id,
            registered_at=stored.registered_at,
            active=True,
        )
        self._registry.put(updated)
        logger.info("Webhook events updated: %s → %s", repo_full_name, events)
        return True

    async def unregister_all(self) -> int:
        """Supprime tous les webhooks de cette instance. Utile pour le cleanup."""
        hooks   = self._registry.list_all()
        removed = 0
        for hook in hooks:
            if hook.instance_id == self._instance_id:
                if await self.unregister(hook.repo_full_name, hook.webhook_id):
                    removed += 1
        return removed

    # ── Private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _parse_repo(repo_full_name: str) -> tuple[str, str]:
        """Parse "owner/repo" → (owner, repo)."""
        parts = repo_full_name.split("/", 1)
        if len(parts) != 2:
            raise ConnectorFatalError(
                f"Invalid repo format '{repo_full_name}' — expected 'owner/repo'"
            )
        return parts[0], parts[1]

    @staticmethod
    def _validate_events(events: list[str]) -> None:
        unknown = set(events) - SUPPORTED_EVENTS
        if unknown:
            logger.warning("Unknown GitHub event types: %s", unknown)

    async def _list_github_hooks(self, owner: str, repo: str) -> list[dict]:
        return await self._client.collect_all(f"/repos/{owner}/{repo}/hooks")
