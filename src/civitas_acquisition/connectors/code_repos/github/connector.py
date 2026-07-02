"""
GitHubConnector v2 — connecteur GitHub complet avec gestion auto des webhooks.

Améliorations v2 :
  - WebhookManager intégré : register/unregister via GitHub API (pattern Activepieces)
  - GitHubOperations exposées : write operations (create_issue, etc.)
  - validate_credentials() avant connexion (fail-fast)
  - Installation repos support pour GitHub App
  - Discussions via GraphQL
  - Webhook event filters configurables

Config options :
  repos            : list[str]  — repos à acquérir ("owner/repo")
  resource_types   : list[str]  — ["files","issues","prs","releases","commits","discussions","repo_meta"]
  branch           : str        — branche (défaut: "main")
  file_patterns    : list[str]  — globs (ex: ["**/*.py", "**/*.md"])
  max_file_size    : int        — défaut 1MB
  include_closed   : bool       — issues/PRs fermées (défaut: True)
  include_drafts   : bool       — PRs draft (défaut: False)
  owner            : str        — pour discover() auto
  is_org           : bool       — True si owner est une org
  webhook_url      : str        — URL à enregistrer pour les webhooks
  webhook_events   : list[str]  — events à surveiller
  auto_webhook     : bool       — créer/supprimer le webhook auto (défaut: False)
  webhook_registry : str        — chemin fichier JSON pour le registry

Credentials :
  token            : PAT ou OAuth2 Bearer
  webhook_secret   : HMAC secret pour vérification inbound
  app_id           : GitHub App ID
  private_key      : Clé RSA PEM (peut avoir \\n échappés — normalisé auto)
  installation_id  : Installation ID
"""
from __future__ import annotations

import json
import logging
import time
from typing import AsyncIterator, Optional

from civitas_acquisition.connectors._base import BaseConnector
from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth
from civitas_acquisition.connectors.code_repos.github.client import GitHubClient
from civitas_acquisition.connectors.code_repos.github.fetcher import GitHubFetcher
from civitas_acquisition.connectors.code_repos.github.mapper import GitHubMapper
from civitas_acquisition.connectors.code_repos.github.operations import GitHubOperations
from civitas_acquisition.connectors.code_repos.github.webhook import (
    GitHubWebhookParser, WebhookEvent,
)
from civitas_acquisition.connectors.code_repos.github.webhook_manager import (
    GitHubWebhookManager, WebhookRegistry, DEFAULT_EVENTS,
)
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import (
    ChannelType, ConnectorManifest, CredentialSpec, RateLimit, SourceCategory,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.connector_errors import ConnectorAuthenticationError

logger = logging.getLogger(__name__)

_DEFAULT_RESOURCE_TYPES = ["files", "issues", "prs", "releases", "commits"]


def _parse_composite_cursor(cursor: Optional[Cursor]) -> dict[str, str]:
    if cursor is None:
        return {}
    try:
        return json.loads(cursor.value)
    except (json.JSONDecodeError, AttributeError):
        return {}


def _make_composite_cursor(
    cursors: dict[str, str], connector_id: str, instance_id: str
) -> Cursor:
    return Cursor(
        value=json.dumps(cursors, sort_keys=True),
        source_type="token",
        connector_id=connector_id,
        instance_id=instance_id,
    )


class GitHubConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="github",
            display_name="GitHub",
            version="2.0.0",
            source_category=SourceCategory.CODE_REPOSITORY,
            supported_channels=frozenset([
                ChannelType.POLLING,
                ChannelType.WEBHOOK,
                ChannelType.MANUAL,
            ]),
            supported_mime_types=frozenset(["*/*"]),
            required_credentials=(
                CredentialSpec(
                    key="token",
                    description="GitHub PAT ou OAuth2 Bearer Token",
                    sensitive=True,
                ),
            ),
            optional_credentials=(
                CredentialSpec(key="webhook_secret",   description="HMAC secret pour webhooks inbound", required=False, sensitive=True),
                CredentialSpec(key="app_id",           description="GitHub App ID",                    required=False, sensitive=False),
                CredentialSpec(key="private_key",      description="Clé RSA PEM GitHub App",           required=False, sensitive=True),
                CredentialSpec(key="installation_id",  description="Installation ID GitHub App",       required=False, sensitive=False),
            ),
            rate_limit=RateLimit(requests_per_second=1.5, burst_size=10),
            max_batch_size=100,
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    # ── Connect ───────────────────────────────────────────────────────────────

    async def _do_connect(self, config: ConnectorConfig) -> None:
        # Auth
        app_id  = config.credentials.get("app_id")
        pem_key = config.credentials.get("private_key")
        inst_id = config.credentials.get("installation_id")

        if app_id and pem_key and inst_id:
            self._auth = GitHubAuth.from_app(app_id, pem_key, inst_id)
            logger.info("GitHub: GitHub App authentication")
        else:
            token = config.get_credential("token")
            self._auth = GitHubAuth.from_pat(token)
            logger.info("GitHub: PAT authentication")

        # Validate credentials (fail-fast, pattern Activepieces)
        valid, err = await self._auth.validate()
        if not valid:
            raise ConnectorAuthenticationError("github", err)

        # Client + sous-modules
        self._client  = GitHubClient(self._auth, timeout_s=config.get_option("timeout_s", 30.0))
        await self._client.open()

        self._fetcher = GitHubFetcher(
            client=self._client,
            max_file_size=config.get_option("max_file_size", 1_048_576),
            include_closed_issues=config.get_option("include_closed", True),
            include_closed_prs=config.get_option("include_closed", True),
            include_drafts=config.get_option("include_drafts", False),
            include_prereleases=config.get_option("include_prereleases", True),
        )
        self._mapper     = GitHubMapper(instance_id=config.instance_id)
        self._operations = GitHubOperations(self._client)
        self._webhook_parser = GitHubWebhookParser(
            secret=config.credentials.get("webhook_secret")
        )

        # Webhook Manager
        registry_path = config.get_option("webhook_registry")
        self._webhook_manager = GitHubWebhookManager(
            client=self._client,
            registry=WebhookRegistry(storage_path=registry_path),
            instance_id=config.instance_id,
        )

        # Options
        self._repos           = config.get_option("repos", [])
        self._resource_types  = config.get_option("resource_types", _DEFAULT_RESOURCE_TYPES)
        self._default_branch  = config.get_option("branch", "main")
        self._file_patterns   = config.get_option("file_patterns")
        self._owner           = config.get_option("owner")
        self._is_org          = config.get_option("is_org", False)
        self._auto_webhook    = config.get_option("auto_webhook", False)
        self._webhook_url     = config.get_option("webhook_url", "")
        self._webhook_events  = config.get_option("webhook_events", DEFAULT_EVENTS)

        # Auto-register webhooks si activé
        if self._auto_webhook and self._webhook_url:
            await self._auto_register_webhooks()

    async def _do_disconnect(self) -> None:
        # Auto-unregister webhooks si activé
        if getattr(self, "_auto_webhook", False):
            removed = await self._webhook_manager.unregister_all()
            if removed > 0:
                logger.info("Auto-unregistered %d webhook(s)", removed)
        if hasattr(self, "_client"):
            await self._client.close()

    # ── Health ────────────────────────────────────────────────────────────────

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            data = await self._client.get("/rate_limit")
            rate = data.get("rate", {}) if data else {}
            return HealthStatus.ok(
                latency_ms=(time.monotonic() - start) * 1000,
                rate_remaining=rate.get("remaining", "?"),
                rate_limit=rate.get("limit", "?"),
            )
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def discover(self) -> DiscoveryResult:
        if self._repos:
            return DiscoveryResult(
                resources=tuple(f"https://github.com/{r}" for r in self._repos),
                total=len(self._repos),
            )
        resources = []
        if self._auth.is_app_auth:
            # GitHub App : utiliser /installation/repositories
            repos = await self._client.get_installation_repos()
            resources = [f"https://github.com/{r['full_name']}" for r in repos]
        else:
            async for repo in self._fetcher.list_repos(
                owner=self._owner if not self._is_org else None,
                org=self._owner if self._is_org else None,
            ):
                resources.append(f"https://github.com/{repo.full_name}")
        return DiscoveryResult(resources=tuple(resources), total=len(resources))

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors         = _parse_composite_cursor(cursor)
        updated_cursors = dict(cursors)
        count           = 0

        repos = self._repos
        if not repos:
            repos = []
            if self._auth.is_app_auth:
                data  = await self._client.get_installation_repos()
                repos = [r["full_name"] for r in data]
            else:
                async for repo in self._fetcher.list_repos(
                    owner=self._owner if not self._is_org else None,
                    org=self._owner if self._is_org else None,
                ):
                    repos.append(repo.full_name)

        for repo in repos:
            if count >= batch_size:
                break
            logger.info("Pulling from GitHub repo: %s", repo)

            if "repo_meta" in self._resource_types:
                repo_obj = await self._fetcher.fetch_repo(repo)
                if repo_obj:
                    yield self._stamped(self._mapper.map_repo(repo_obj), updated_cursors)
                    count += 1

            if "files" in self._resource_types and count < batch_size:
                since_tree = cursors.get(f"files:{repo}")
                async for file_info, content in self._fetcher.fetch_files(
                    repo=repo, branch=self._default_branch,
                    since_tree_sha=since_tree, file_patterns=self._file_patterns,
                ):
                    if count >= batch_size: break
                    doc = self._mapper.map_file(file_info, content, tree_sha=file_info.sha)
                    updated_cursors[f"files:{repo}"] = file_info.sha
                    yield self._stamped(doc, updated_cursors)
                    count += 1

            if "issues" in self._resource_types and count < batch_size:
                since = cursors.get(f"issues:{repo}")
                async for issue in self._fetcher.fetch_issues(repo=repo, since=since):
                    if count >= batch_size: break
                    doc = self._mapper.map_issue(issue)
                    updated_cursors[f"issues:{repo}"] = issue.updated_at
                    yield self._stamped(doc, updated_cursors)
                    count += 1

            if "prs" in self._resource_types and count < batch_size:
                since = cursors.get(f"prs:{repo}")
                async for pr in self._fetcher.fetch_pull_requests(repo=repo, since=since):
                    if count >= batch_size: break
                    doc = self._mapper.map_pull_request(pr)
                    updated_cursors[f"prs:{repo}"] = pr.updated_at
                    yield self._stamped(doc, updated_cursors)
                    count += 1

            if "releases" in self._resource_types and count < batch_size:
                since_id = int(cursors[f"releases:{repo}"]) if f"releases:{repo}" in cursors else None
                async for release in self._fetcher.fetch_releases(repo=repo, since_id=since_id):
                    if count >= batch_size: break
                    doc = self._mapper.map_release(release)
                    updated_cursors[f"releases:{repo}"] = str(release.id)
                    yield self._stamped(doc, updated_cursors)
                    count += 1

            if "commits" in self._resource_types and count < batch_size:
                since = cursors.get(f"commits:{repo}")
                async for commit in self._fetcher.fetch_commits(
                    repo=repo, branch=self._default_branch, since=since,
                ):
                    if count >= batch_size: break
                    doc = self._mapper.map_commit(commit)
                    updated_cursors[f"commits:{repo}"] = commit.author_date
                    yield self._stamped(doc, updated_cursors)
                    count += 1

            if "discussions" in self._resource_types and count < batch_size:
                owner, repo_name = repo.split("/", 1)
                discussions = await self._operations.get_discussions(owner, repo_name)
                for disc in discussions:
                    if count >= batch_size: break
                    content = json.dumps(disc, ensure_ascii=False).encode()
                    yield RawDocument.create(
                        instance_id=self.instance_id,
                        connector_id="github",
                        uri=disc.get("url", ""),
                        content=content,
                        content_type="application/json",
                        version=disc.get("updatedAt"),
                        cursor=_make_composite_cursor(updated_cursors, "github", self.instance_id),
                        tags=("discussion", f"repo:{repo}"),
                        source_metadata={"resource_type": "discussion", "repo": repo},
                    )
                    count += 1

        logger.info("GitHub pull completed: %d documents", count)

    # ── Webhook inbound ───────────────────────────────────────────────────────

    def parse_webhook_event(
        self, body: bytes, headers: dict[str, str]
    ) -> Optional[WebhookEvent]:
        try:
            event = self._webhook_parser.parse(body=body, headers=headers)
            return event if event.should_acquire else None
        except ValueError as exc:
            logger.warning("Invalid webhook: %s", exc)
            return None

    # ── Webhook management (auto) ─────────────────────────────────────────────

    async def register_webhooks(
        self,
        repos: list[str] | None = None,
        webhook_url: str | None = None,
        events: list[str] | None = None,
        secret: Optional[str] = None,
    ) -> dict[str, int]:
        """
        Crée des webhooks GitHub sur les repos configurés.
        Retourne {repo_full_name: webhook_id}.
        Pattern Activepieces : onEnable
        """
        target_repos = repos or self._repos
        target_url   = webhook_url or self._webhook_url
        target_events = events or self._webhook_events

        if not target_url:
            raise ValueError("webhook_url must be provided")

        results = {}
        for repo in target_repos:
            hook = await self._webhook_manager.register(
                repo_full_name=repo,
                webhook_url=target_url,
                events=target_events,
                secret=secret,
            )
            results[repo] = hook.webhook_id
        return results

    async def unregister_webhooks(
        self, repos: list[str] | None = None
    ) -> dict[str, bool]:
        """
        Supprime les webhooks CIVITAS sur les repos configurés.
        Pattern Activepieces : onDisable
        """
        target_repos = repos or self._repos
        results = {}
        for repo in target_repos:
            results[repo] = await self._webhook_manager.unregister(repo)
        return results

    # ── Operations (write) ────────────────────────────────────────────────────

    @property
    def ops(self) -> GitHubOperations:
        """Accès aux write operations GitHub."""
        return self._operations

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _auto_register_webhooks(self) -> None:
        for repo in self._repos:
            try:
                await self._webhook_manager.register(
                    repo_full_name=repo,
                    webhook_url=self._webhook_url,
                    events=self._webhook_events,
                    secret=self.config.credentials.get("webhook_secret"),
                )
            except Exception as exc:
                logger.warning("Auto-register webhook failed for %s: %s", repo, exc)

    def _stamped(self, doc: RawDocument, cursors: dict) -> RawDocument:
        import dataclasses
        return dataclasses.replace(
            doc,
            cursor=_make_composite_cursor(cursors, "github", self.instance_id),
        )
