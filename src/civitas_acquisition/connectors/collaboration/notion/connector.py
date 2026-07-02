"""
NotionConnector — connecteur Notion complet.

Config options :
  resource_types : list["pages", "databases"]  — défaut: ["pages"]
  database_ids   : list[str]  — UUIDs databases spécifiques (optionnel)
  fetch_content  : bool  — récupérer le contenu Markdown des pages (défaut: True)
  max_depth      : int   — profondeur max de récursion des blocs (défaut: 5)

Credentials :
  token          : Notion Integration Token (secret_xxxx)
"""
from __future__ import annotations
import logging
import time
from typing import AsyncIterator, Optional
from civitas_acquisition.connectors._base import BaseConnector
from civitas_acquisition.connectors.collaboration.notion.client import NotionClient
from civitas_acquisition.connectors.collaboration.notion.fetcher import NotionFetcher
from civitas_acquisition.connectors.collaboration.notion.mapper import NotionMapper
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import (
    ChannelType, ConnectorManifest, CredentialSpec, RateLimit, SourceCategory,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.raw_document import RawDocument

logger = logging.getLogger(__name__)


class NotionConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="notion",
            display_name="Notion",
            version="1.0.0",
            source_category=SourceCategory.COLLABORATION,
            supported_channels=frozenset([ChannelType.POLLING, ChannelType.MANUAL]),
            supported_mime_types=frozenset(["text/markdown", "application/json"]),
            required_credentials=(
                CredentialSpec(key="token", description="Notion Integration Secret Token"),
            ),
            rate_limit=RateLimit(requests_per_second=3.0, burst_size=10),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    async def _do_connect(self, config: ConnectorConfig) -> None:
        token = config.get_credential("token")
        timeout_s = config.get_option("timeout_s", 30.0)
        self._client = NotionClient(token=token, timeout_s=timeout_s)
        await self._client.open()
        self._fetcher = NotionFetcher(self._client)
        self._mapper = NotionMapper(instance_id=config.instance_id)
        self._resource_types: list[str] = config.get_option("resource_types", ["pages"])
        self._database_ids: list[str] = config.get_option("database_ids", [])
        self._fetch_content: bool = config.get_option("fetch_content", True)

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_client"):
            await self._client.close()

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            await self._client.get("/users/me")
            return HealthStatus.ok(latency_ms=(time.monotonic() - start) * 1000)
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    async def discover(self) -> DiscoveryResult:
        resources = []
        async for page in self._fetcher.search_pages():
            resources.append(page.url)
        async for db in self._fetcher.search_databases():
            resources.append(db.url)
        return DiscoveryResult(resources=tuple(resources), total=len(resources))

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        since = cursor.value if cursor else None
        count = 0

        # ── Pages ─────────────────────────────────────────────────────────────
        if "pages" in self._resource_types:
            async for page in self._fetcher.search_pages(since=since):
                if count >= batch_size:
                    break
                if page.archived:
                    continue
                markdown = ""
                if self._fetch_content:
                    blocks = await self._fetcher.fetch_page_content(page.id)
                    markdown = self._fetcher.blocks_to_markdown(blocks)
                doc = self._mapper.map_page(page, markdown)
                yield doc
                count += 1

        # ── Databases ─────────────────────────────────────────────────────────
        if "databases" in self._resource_types:
            db_ids = self._database_ids
            if not db_ids:
                db_ids = []
                async for db in self._fetcher.search_databases():
                    db_ids.append(db.id)

            for db_id in db_ids:
                if count >= batch_size:
                    break
                try:
                    db_data = await self._client.get(f"/databases/{db_id}")
                    from civitas_acquisition.connectors.collaboration.notion.models import NotionDatabase
                    db_obj = NotionDatabase.from_api(db_data)
                    rows = []
                    async for row in self._fetcher.query_database(db_id, since=since):
                        rows.append(row)
                    doc = self._mapper.map_database(db_obj, rows)
                    yield doc
                    count += 1
                except Exception as exc:
                    logger.warning("Error fetching database %s: %s", db_id, exc)

        logger.info("Notion pull completed: %d documents", count)
