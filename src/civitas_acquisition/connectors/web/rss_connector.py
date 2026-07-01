"""
RSSConnector — connecteur RSS/Atom.

Premier connecteur de référence concret.
Illustre le pattern complet : manifest, connect, pull, healthcheck, discover.
Sans dépendance externe (stdlib xml uniquement) — fonctionne immédiatement.

Sources supportées : RSS 2.0, Atom 1.0
Canal : POLLING (vérification périodique des nouveaux items)
"""
from __future__ import annotations

import hashlib
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import AsyncIterator, Optional
from xml.etree import ElementTree as ET

from civitas_acquisition.connectors._base import BaseConnector
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
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorNetworkError,
    ConnectorAuthenticationError,
)


class RSSConnector(BaseConnector):
    """
    Connecteur RSS/Atom.

    Config options :
      feed_urls  : list[str]  — URLs des flux à surveiller
      user_agent : str        — User-Agent HTTP (optionnel)

    Credentials : aucun (flux publics). Pour flux privés : "basic_auth" optionnel.
    """

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="rss",
            display_name="RSS / Atom Feed",
            version="1.0.0",
            source_category=SourceCategory.WEB,
            supported_channels=frozenset([ChannelType.POLLING]),
            supported_mime_types=frozenset([
                "application/rss+xml",
                "application/atom+xml",
                "text/xml",
                "application/xml",
            ]),
            required_credentials=(),
            optional_credentials=(
                CredentialSpec(
                    key="basic_auth",
                    description="Base64 basic auth pour flux privés",
                    required=False,
                ),
            ),
            rate_limit=RateLimit(requests_per_second=0.5, burst_size=3),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    async def _do_connect(self, config: ConnectorConfig) -> None:
        self._feed_urls: list[str] = config.get_option("feed_urls", [])
        self._user_agent: str = config.get_option("user_agent", "CIVITAS-Acquisition/1.0")
        self._basic_auth: Optional[str] = config.credentials.get("basic_auth")

        if not self._feed_urls:
            raise ConnectorAuthenticationError(
                "rss", "feed_urls option is required"
            )

    async def healthcheck(self) -> HealthStatus:
        if not self._feed_urls:
            return HealthStatus.fail("No feed URLs configured")
        start = time.monotonic()
        try:
            self._fetch_feed(self._feed_urls[0])
            return HealthStatus.ok(latency_ms=(time.monotonic() - start) * 1000)
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    async def discover(self) -> DiscoveryResult:
        return DiscoveryResult(
            resources=tuple(self._feed_urls),
            total=len(self._feed_urls),
        )

    async def _do_pull(
        self,
        cursor: Optional[Cursor] = None,
        batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        since_ts = float(cursor.value) if cursor else 0.0
        count = 0

        for feed_url in self._feed_urls:
            if count >= batch_size:
                break
            try:
                raw_xml, content_type = self._fetch_feed(feed_url)
                items = self._parse_feed(raw_xml)

                latest_ts = since_ts
                for item in items:
                    if count >= batch_size:
                        break
                    item_ts = item.get("published_ts", 0.0)
                    if item_ts <= since_ts:
                        continue

                    uri = item.get("link") or item.get("id") or f"{feed_url}#{hashlib.md5(str(item).encode()).hexdigest()[:8]}"
                    content = item.get("content") or item.get("summary") or ""
                    content_bytes = content.encode("utf-8")

                    new_cursor = Cursor(
                        value=str(item_ts),
                        source_type="timestamp",
                        connector_id="rss",
                        instance_id=self.instance_id,
                    )

                    yield RawDocument.create(
                        instance_id=self.instance_id,
                        connector_id="rss",
                        uri=uri,
                        content=content_bytes,
                        content_type="text/plain",
                        version=None,
                        cursor=new_cursor,
                        source_metadata={
                            "feed_url": feed_url,
                            "title": item.get("title", ""),
                            "published": item.get("published", ""),
                            "author": item.get("author", ""),
                        },
                    )
                    count += 1
                    latest_ts = max(latest_ts, item_ts)

            except urllib.error.URLError as exc:
                raise ConnectorNetworkError("rss", url=feed_url, cause=str(exc)) from exc

    def _fetch_feed(self, url: str) -> tuple[bytes, str]:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": self._user_agent},
        )
        if self._basic_auth:
            req.add_header("Authorization", f"Basic {self._basic_auth}")
        with urllib.request.urlopen(req, timeout=10) as response:
            content_type = response.headers.get("Content-Type", "application/xml")
            return response.read(), content_type

    def _parse_feed(self, raw_xml: bytes) -> list[dict]:
        """Parse RSS 2.0 et Atom 1.0 vers une liste de dicts normalisés."""
        items: list[dict] = []
        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError:
            return items

        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # RSS 2.0
        for item in root.findall(".//item"):
            pub_date = item.findtext("pubDate", "")
            items.append({
                "title":        item.findtext("title", ""),
                "link":         item.findtext("link", ""),
                "content":      item.findtext("description", ""),
                "author":       item.findtext("author", ""),
                "published":    pub_date,
                "published_ts": self._parse_date(pub_date),
            })

        # Atom 1.0
        for entry in root.findall("atom:entry", ns):
            updated = entry.findtext("atom:updated", "", ns)
            link_el = entry.find("atom:link", ns)
            content_el = entry.find("atom:content", ns) or entry.find("atom:summary", ns)
            items.append({
                "title":        entry.findtext("atom:title", "", ns),
                "link":         link_el.get("href", "") if link_el is not None else "",
                "id":           entry.findtext("atom:id", "", ns),
                "content":      content_el.text or "" if content_el is not None else "",
                "published":    updated,
                "published_ts": self._parse_date(updated),
            })

        return items

    def _parse_date(self, date_str: str) -> float:
        """Convertit une date RSS/Atom en timestamp Unix. 0.0 si invalide."""
        if not date_str:
            return 0.0
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except ValueError:
                continue
        return 0.0
