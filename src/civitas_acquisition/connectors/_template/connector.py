"""
Template de connecteur CIVITAS.

Copier ce répertoire pour créer un nouveau connecteur.
Renommer SourceName par le nom de votre source (GitHub, Notion, S3...).

Structure interne d'un connecteur :
  auth.py       — gestion de l'authentification et des tokens
  discovery.py  — listing des ressources disponibles dans la source
  fetcher.py    — récupération du contenu brut
  mapper.py     — mapping du format natif vers RawDocument
  connector.py  — assemblage et implémentation de ConnectorPort (ce fichier)

Cette séparation évite d'avoir un connecteur monolithique de 500 lignes.
Chaque responsabilité évolue indépendamment.
"""

from __future__ import annotations

import time
from typing import AsyncIterator, Optional

from civitas_acquisition.contracts.ports.connector_port import ConnectorPort
from civitas_acquisition.contracts.models.connector_manifest import (
    ConnectorManifest,
    ChannelType,
    SourceCategory,
    CredentialSpec,
    RateLimit,
)
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult

# Importer les sous-modules internes au connecteur
# from .auth import SourceNameAuth
# from .discovery import SourceNameDiscovery
# from .fetcher import SourceNameFetcher
# from .mapper import SourceNameMapper


class TemplateConnector(ConnectorPort):
    """
    Connecteur template. Remplacer TemplateConnector par SourceNameConnector.

    Chaque méthode délègue à son sous-module spécialisé.
    Le connecteur lui-même ne contient que de la coordination.
    """

    # ── Manifest — AUCUN appel réseau ─────────────────────────────────────────

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="template",
            display_name="Template Source",
            version="0.1.0",
            source_category=SourceCategory.CUSTOM,
            supported_channels=frozenset([ChannelType.POLLING]),
            supported_mime_types=frozenset(["text/plain"]),
            required_credentials=(
                CredentialSpec(key="api_key", description="API Key"),
            ),
            rate_limit=RateLimit(requests_per_second=2.0, burst_size=5),
            supports_cursor=True,
            supports_delta=False,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self, config: ConnectorConfig) -> None:
        self._config = config
        # Initialiser les sous-modules :
        # self._auth = SourceNameAuth(config.get_credential("api_key"))
        # self._fetcher = SourceNameFetcher(self._auth)
        # self._mapper = SourceNameMapper(config.instance_id, self.manifest().connector_id)
        # await self._auth.verify()

    async def disconnect(self) -> None:
        # Nettoyer les resources réseau
        # await self._fetcher.close()
        pass

    # ── Health ────────────────────────────────────────────────────────────────

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            # Appel léger à la source pour vérifier la disponibilité
            # await self._auth.ping()
            return HealthStatus.ok(latency_ms=(time.monotonic() - start) * 1000)
        except Exception as e:
            return HealthStatus.fail(str(e))

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def discover(self) -> DiscoveryResult:
        # Déléguer à SourceNameDiscovery
        # resources = await self._discovery.list_all()
        # return DiscoveryResult(resources=tuple(r.uri for r in resources), total=len(resources))
        return DiscoveryResult(resources=(), total=0)

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def pull(
        self,
        cursor: Optional[Cursor] = None,
        batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        # Déléguer au Fetcher et au Mapper
        # async for raw_item in self._fetcher.fetch(since=cursor, limit=batch_size):
        #     yield self._mapper.to_raw_document(raw_item)
        return
        yield  # Rendre cette méthode un async generator même vide
