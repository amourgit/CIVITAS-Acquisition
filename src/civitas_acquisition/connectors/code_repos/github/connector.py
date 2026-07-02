"""
GitHubConnector — connecteur GitHub complet.

Assemble auth, client, fetcher et mapper pour exposer l'interface ConnectorPort.
Supporte : POLLING, WEBHOOK, MANUAL.

Configuration (options) :
  repos           : list[str]  — repos à acquérir ("owner/repo")
  resource_types  : list[str]  — ["files", "issues", "prs", "releases", "commits", "repo_meta"]
  branch          : str        — branche par défaut ("main")
  since           : str        — ISO-8601 pour le delta initial (issues/prs/commits)
  file_patterns   : list[str]  — glob patterns de fichiers (ex: ["**/*.py", "**/*.md"])
  max_file_size   : int        — taille max d'un fichier en bytes (défaut: 1MB)
  include_closed  : bool       — inclure issues/PRs fermées (défaut: True)
  include_drafts  : bool       — inclure PRs en draft (défaut: False)
  owner           : str        — owner pour discover() (user ou org)
  is_org          : bool       — True si owner est une organisation

Credentials :
  token           : Personal Access Token ou GitHub App installation token
  webhook_secret  : Secret pour vérification HMAC des webhooks (optionnel)
  app_id          : GitHub App ID (si auth type App)
  private_key     : Clé privée RSA GitHub App (PEM)
  installation_id : Installation ID de l'App sur le repo/org
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
from civitas_acquisition.connectors.code_repos.github.webhook import (
    GitHubWebhookParser,
    WebhookEvent,
)
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import (
    ChannelType,
    ConnectorManifest,
    CredentialSpec,
    RateLimit,
    SourceCategory,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.connector_errors import ConnectorAuthenticationError

logger = logging.getLogger(__name__)

# Curseur composite — un sous-curseur par type de ressource
_DEFAULT_RESOURCE_TYPES = ["files", "issues", "prs", "releases", "commits"]


def _parse_composite_cursor(cursor: Optional[Cursor]) -> dict[str, str]:
    """Désérialise le curseur composite JSON."""
    if cursor is None:
        return {}
    try:
        return json.loads(cursor.value)
    except (json.JSONDecodeError, AttributeError):
        return {}


def _make_composite_cursor(
    cursors: dict[str, str],
    connector_id: str,
    instance_id: str,
) -> Cursor:
    """Sérialise le curseur composite en JSON."""
    return Cursor(
        value=json.dumps(cursors, sort_keys=True),
        source_type="token",
        connector_id=connector_id,
        instance_id=instance_id,
    )


class GitHubConnector(BaseConnector):
    """
    Connecteur GitHub — acquisition complète de repositories.

    Supporte tous les types de ressources GitHub et les deux modes
    d'authentification principaux (PAT et GitHub App).
    """

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="github",
            display_name="GitHub",
            version="1.0.0",
            source_category=SourceCategory.CODE_REPOSITORY,
            supported_channels=frozenset([
                ChannelType.POLLING,
                ChannelType.WEBHOOK,
                ChannelType.MANUAL,
            ]),
            supported_mime_types=frozenset(["*/*"]),  # tous types de fichiers
            required_credentials=(
                CredentialSpec(
                    key="token",
                    description="GitHub Personal Access Token (PAT) ou App Installation Token",
                    sensitive=True,
                ),
            ),
            optional_credentials=(
                CredentialSpec(
                    key="webhook_secret",
                    description="Secret HMAC pour vérification des webhooks",
                    required=False,
                    sensitive=True,
                ),
                CredentialSpec(
                    key="app_id",
                    description="GitHub App ID (si auth via App)",
                    required=False,
                    sensitive=False,
                ),
                CredentialSpec(
                    key="private_key",
                    description="Clé privée RSA de l'App GitHub (PEM)",
                    required=False,
                    sensitive=True,
                ),
                CredentialSpec(
                    key="installation_id",
                    description="Installation ID de l'App GitHub",
                    required=False,
                    sensitive=False,
                ),
            ),
            rate_limit=RateLimit(requests_per_second=1.5, burst_size=10),
            max_batch_size=100,
            max_concurrency=1,
            supports_cursor=True,
            supports_delta=True,
            supports_streaming=False,
            supports_discovery=True,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _do_connect(self, config: ConnectorConfig) -> None:
        # Authentification
        app_id = config.credentials.get("app_id")
        private_key = config.credentials.get("private_key")
        installation_id = config.credentials.get("installation_id")

        if app_id and private_key and installation_id:
            self._auth = GitHubAuth.from_app(
                app_id=app_id,
                private_key=private_key,
                installation_id=installation_id,
            )
            logger.info("GitHub connector: using App authentication")
        else:
            token = config.get_credential("token")
            self._auth = GitHubAuth.from_pat(token)
            logger.info("GitHub connector: using PAT authentication")

        # Client HTTP
        timeout_s = config.get_option("timeout_s", 30.0)
        self._client = GitHubClient(self._auth, timeout_s=timeout_s)
        await self._client.open()

        # Fetcher
        self._fetcher = GitHubFetcher(
            client=self._client,
            max_file_size=config.get_option("max_file_size", 1_048_576),
            include_closed_issues=config.get_option("include_closed", True),
            include_closed_prs=config.get_option("include_closed", True),
            include_drafts=config.get_option("include_drafts", False),
            include_prereleases=config.get_option("include_prereleases", True),
        )

        # Mapper
        self._mapper = GitHubMapper(
            instance_id=config.instance_id,
            connector_id="github",
        )

        # Webhook parser
        webhook_secret = config.credentials.get("webhook_secret")
        self._webhook_parser = GitHubWebhookParser(secret=webhook_secret)

        # Config options
        self._repos: list[str] = config.get_option("repos", [])
        self._resource_types: list[str] = config.get_option(
            "resource_types", _DEFAULT_RESOURCE_TYPES
        )
        self._default_branch: str = config.get_option("branch", "main")
        self._file_patterns: Optional[list[str]] = config.get_option("file_patterns")
        self._owner: Optional[str] = config.get_option("owner")
        self._is_org: bool = config.get_option("is_org", False)

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_client"):
            await self._client.close()

    # ── Health ────────────────────────────────────────────────────────────────

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            data = await self._client.get("/rate_limit")
            rate_info = data.get("rate", {}) if data else {}
            latency_ms = (time.monotonic() - start) * 1000
            return HealthStatus.ok(
                latency_ms=latency_ms,
                rate_limit_remaining=rate_info.get("remaining", "?"),
                rate_limit_limit=rate_info.get("limit", "?"),
            )
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def discover(self) -> DiscoveryResult:
        """
        Liste tous les repositories accessibles.
        Si repos est configuré : retourne juste ces repos.
        Sinon : liste tous les repos de l'owner/org.
        """
        if self._repos:
            return DiscoveryResult(
                resources=tuple(
                    f"https://github.com/{r}" for r in self._repos
                ),
                total=len(self._repos),
            )

        resources = []
        async for repo in self._fetcher.list_repos(
            owner=self._owner if not self._is_org else None,
            org=self._owner if self._is_org else None,
        ):
            resources.append(f"https://github.com/{repo.full_name}")

        return DiscoveryResult(
            resources=tuple(resources),
            total=len(resources),
        )

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def _do_pull(
        self,
        cursor: Optional[Cursor] = None,
        batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors = _parse_composite_cursor(cursor)
        updated_cursors = dict(cursors)
        count = 0

        # Résoudre la liste de repos
        repos = self._repos
        if not repos:
            repos = []
            async for repo in self._fetcher.list_repos(
                owner=self._owner if not self._is_org else None,
                org=self._owner if self._is_org else None,
            ):
                repos.append(repo.full_name)

        for repo in repos:
            if count >= batch_size:
                break

            logger.info("Pulling from GitHub repo: %s", repo)

            # ── Metadata du repo ─────────────────────────────────────────────
            if "repo_meta" in self._resource_types:
                repo_obj = await self._fetcher.fetch_repo(repo)
                if repo_obj:
                    doc = self._mapper.map_repo(repo_obj)
                    doc = self._with_cursor(doc, updated_cursors)
                    yield doc
                    count += 1

            # ── Fichiers ─────────────────────────────────────────────────────
            if "files" in self._resource_types and count < batch_size:
                since_tree = cursors.get(f"files:{repo}")
                async for file_info, content in self._fetcher.fetch_files(
                    repo=repo,
                    branch=self._default_branch,
                    since_tree_sha=since_tree,
                    file_patterns=self._file_patterns,
                ):
                    if count >= batch_size:
                        break
                    tree_sha = file_info.sha  # blob SHA — le tree SHA est dans metadata
                    doc = self._mapper.map_file(
                        file_info, content,
                        tree_sha=file_info.sha,
                    )
                    # Mettre à jour le curseur de fichiers
                    updated_cursors[f"files:{repo}"] = file_info.sha
                    doc = self._with_cursor(doc, updated_cursors)
                    yield doc
                    count += 1

            # ── Issues ───────────────────────────────────────────────────────
            if "issues" in self._resource_types and count < batch_size:
                since = cursors.get(f"issues:{repo}")
                async for issue in self._fetcher.fetch_issues(repo=repo, since=since):
                    if count >= batch_size:
                        break
                    doc = self._mapper.map_issue(issue)
                    updated_cursors[f"issues:{repo}"] = issue.updated_at
                    doc = self._with_cursor(doc, updated_cursors)
                    yield doc
                    count += 1

            # ── Pull Requests ────────────────────────────────────────────────
            if "prs" in self._resource_types and count < batch_size:
                since = cursors.get(f"prs:{repo}")
                async for pr in self._fetcher.fetch_pull_requests(repo=repo, since=since):
                    if count >= batch_size:
                        break
                    doc = self._mapper.map_pull_request(pr)
                    updated_cursors[f"prs:{repo}"] = pr.updated_at
                    doc = self._with_cursor(doc, updated_cursors)
                    yield doc
                    count += 1

            # ── Releases ─────────────────────────────────────────────────────
            if "releases" in self._resource_types and count < batch_size:
                since_id = int(cursors[f"releases:{repo}"]) if f"releases:{repo}" in cursors else None
                async for release in self._fetcher.fetch_releases(repo=repo, since_id=since_id):
                    if count >= batch_size:
                        break
                    doc = self._mapper.map_release(release)
                    updated_cursors[f"releases:{repo}"] = str(release.id)
                    doc = self._with_cursor(doc, updated_cursors)
                    yield doc
                    count += 1

            # ── Commits ──────────────────────────────────────────────────────
            if "commits" in self._resource_types and count < batch_size:
                since = cursors.get(f"commits:{repo}")
                async for commit in self._fetcher.fetch_commits(
                    repo=repo, branch=self._default_branch, since=since
                ):
                    if count >= batch_size:
                        break
                    doc = self._mapper.map_commit(commit)
                    updated_cursors[f"commits:{repo}"] = commit.author_date
                    doc = self._with_cursor(doc, updated_cursors)
                    yield doc
                    count += 1

        logger.info("GitHub pull completed: %d documents", count)

    # ── Webhook ───────────────────────────────────────────────────────────────

    def parse_webhook_event(
        self,
        body: bytes,
        headers: dict[str, str],
    ) -> Optional[WebhookEvent]:
        """
        Parse et valide un événement webhook entrant.
        Retourne None si l'événement ne nécessite pas d'acquisition.
        """
        try:
            event = self._webhook_parser.parse(body=body, headers=headers)
            if not event.should_acquire:
                logger.debug(
                    "Webhook event %s/%s ignored (should_acquire=False)",
                    event.event_type, event.action,
                )
                return None
            return event
        except ValueError as exc:
            logger.warning("Invalid webhook: %s", exc)
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _with_cursor(
        self, doc: RawDocument, cursors: dict[str, str]
    ) -> RawDocument:
        """Retourne une copie du document avec le curseur composite mis à jour."""
        import dataclasses
        composite = _make_composite_cursor(
            cursors,
            connector_id="github",
            instance_id=self.instance_id,
        )
        return dataclasses.replace(doc, cursor=composite)
