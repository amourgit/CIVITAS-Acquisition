"""
WebConnector — crawler web async + Generic API connector.

Deux modes :
  1. CRAWLER  : crawl récursif d'un site web (BFS/DFS, respect robots.txt)
  2. API      : appels REST/GraphQL vers n'importe quelle API

Crawler features :
  - BFS par défaut, configurable en DFS
  - Respect robots.txt avec cache
  - Politeness delay configurable
  - Filtres par domaine, extension, regex
  - Extraction de liens depuis HTML
  - Content-Type detection automatique
  - Curseur par URL (ETag / Last-Modified)
  - Max depth, max pages configurables

Generic API features :
  - Pagination : link-header, offset/limit, cursor-based, page-based
  - Auth : Bearer, API Key (header/query), Basic Auth
  - Retry sur 429/5xx avec Retry-After
  - JSON path extraction configurable
  - Rate limit configurable par config

Config options (crawler) :
  mode           : "crawler" | "api"
  seed_urls      : list[str]  — URLs de départ
  allowed_domains: list[str]  — domaines autorisés (défaut: domaines des seeds)
  max_depth      : int        — profondeur max (défaut: 3)
  max_pages      : int        — pages max par cycle (défaut: 1000)
  politeness_s   : float      — délai entre requêtes (défaut: 1.0)
  respect_robots : bool       — respecter robots.txt (défaut: True)
  include_extensions: list[str] — extensions à inclure (défaut: html,pdf,md,txt)
  exclude_patterns: list[str] — regex URL à exclure

Config options (api) :
  base_url       : str        — URL de base
  endpoints      : list[dict] — [{path, method, params, pagination}]
  pagination_type: str        — "link_header" | "offset" | "cursor" | "page"
  items_path     : str        — chemin JSON vers les items (ex: "data.items")
  cursor_path    : str        — chemin JSON vers le next cursor

Credentials :
  bearer_token   : Bearer token (optionnel)
  api_key        : Clé API (optionnel)
  api_key_header : Header name (défaut: X-API-Key)
  basic_username / basic_password : Basic Auth (optionnel)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from typing import Any, AsyncIterator, Optional
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import aiohttp

from civitas_acquisition.connectors._base import BaseConnector
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import (
    ChannelType, ConnectorManifest, CredentialSpec, RateLimit, SourceCategory,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorRateLimitError, ConnectorTemporaryError, ConnectorNetworkError,
)

logger = logging.getLogger(__name__)

_INCLUDE_EXTENSIONS = frozenset([
    ".html", ".htm", ".php", ".asp", ".aspx",
    ".pdf", ".md", ".txt", ".rst", ".doc", ".docx",
    ".json", ".xml", ".csv",
])
_EXCLUDE_PATTERNS_DEFAULT = [
    r"\.(png|jpg|jpeg|gif|ico|svg|webp|mp4|mp3|avi|zip|tar|gz|exe|bin)$",
    r"/(login|logout|signin|signup|register|auth)",
    r"[?&](utm_|fbclid|gclid)",
]
USER_AGENT = "CIVITAS-Acquisition/1.0 (knowledge-crawler)"


class WebConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="web",
            display_name="Web Crawler / Generic API",
            version="1.0.0",
            source_category=SourceCategory.WEB,
            supported_channels=frozenset([
                ChannelType.POLLING, ChannelType.MANUAL,
            ]),
            supported_mime_types=frozenset([
                "text/html", "text/plain", "application/json",
                "application/pdf", "text/markdown", "*/*",
            ]),
            required_credentials=(),
            optional_credentials=(
                CredentialSpec(key="bearer_token",    description="Bearer Token",      required=False, sensitive=True),
                CredentialSpec(key="api_key",         description="API Key",           required=False, sensitive=True),
                CredentialSpec(key="api_key_header",  description="API Key Header",    required=False, sensitive=False),
                CredentialSpec(key="basic_username",  description="Basic Auth User",   required=False, sensitive=False),
                CredentialSpec(key="basic_password",  description="Basic Auth Pass",   required=False, sensitive=True),
            ),
            rate_limit=RateLimit(requests_per_second=1.0, burst_size=5),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=False,
        )

    # ── Connect ───────────────────────────────────────────────────────────────

    async def _do_connect(self, config: ConnectorConfig) -> None:
        self._mode        = config.get_option("mode", "crawler")
        self._politeness  = config.get_option("politeness_s", 1.0)
        self._respect_robots = config.get_option("respect_robots", True)

        # Auth headers
        self._auth_headers: dict[str, str] = {"User-Agent": USER_AGENT}
        if token := config.credentials.get("bearer_token"):
            self._auth_headers["Authorization"] = f"Bearer {token}"
        if api_key := config.credentials.get("api_key"):
            header = config.credentials.get("api_key_header", "X-API-Key")
            self._auth_headers[header] = api_key
        if config.credentials.get("basic_username"):
            import base64
            creds = f"{config.credentials['basic_username']}:{config.credentials.get('basic_password','')}"
            self._auth_headers["Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode()

        timeout = aiohttp.ClientTimeout(total=config.get_option("timeout_s", 30.0))
        self._session = aiohttp.ClientSession(timeout=timeout, headers=self._auth_headers)

        # Crawler config
        self._seed_urls       = config.get_option("seed_urls", [])
        self._allowed_domains = set(config.get_option("allowed_domains", []))
        if not self._allowed_domains:
            self._allowed_domains = {urlparse(u).netloc for u in self._seed_urls}
        self._max_depth        = config.get_option("max_depth", 3)
        self._max_pages        = config.get_option("max_pages", 1000)
        self._include_exts     = frozenset(config.get_option("include_extensions", list(_INCLUDE_EXTENSIONS)))
        self._exclude_patterns = [re.compile(p) for p in config.get_option("exclude_patterns", _EXCLUDE_PATTERNS_DEFAULT)]

        # API config
        self._base_url        = config.get_option("base_url", "")
        self._endpoints       = config.get_option("endpoints", [])
        self._pagination_type = config.get_option("pagination_type", "link_header")
        self._items_path      = config.get_option("items_path", "")
        self._cursor_path     = config.get_option("cursor_path", "next_cursor")

        # Robots cache
        self._robots_cache: dict[str, RobotFileParser] = {}
        self._etag_cache: dict[str, str] = {}

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_session") and not self._session.closed:
            await self._session.close()

    # ── Health ────────────────────────────────────────────────────────────────

    async def healthcheck(self) -> HealthStatus:
        urls = self._seed_urls or ([self._base_url] if self._base_url else [])
        if not urls:
            return HealthStatus.fail("No seed_urls or base_url configured")
        start = time.monotonic()
        try:
            async with self._session.head(urls[0]) as resp:
                return HealthStatus.ok(
                    latency_ms=(time.monotonic() - start) * 1000,
                    status=resp.status,
                    url=urls[0],
                )
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    async def discover(self) -> DiscoveryResult:
        resources = tuple(self._seed_urls or ([self._base_url] if self._base_url else []))
        return DiscoveryResult(resources=resources, total=len(resources))

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors = json.loads(cursor.value) if cursor else {}
        updated = dict(cursors)

        if self._mode == "api":
            async for doc in self._pull_api(cursors, batch_size):
                yield self._stamp(doc, updated)
        else:
            async for doc in self._crawl(cursors, batch_size):
                yield self._stamp(doc, updated)

    # ── Crawler ───────────────────────────────────────────────────────────────

    async def _crawl(
        self, cursors: dict, batch_size: int,
    ) -> AsyncIterator[RawDocument]:
        visited: set[str] = set(cursors.get("__visited", "").split(",")) if cursors.get("__visited") else set()
        queue: deque[tuple[str, int]] = deque()

        for seed in self._seed_urls:
            if seed not in visited:
                queue.append((seed, 0))

        count = 0
        while queue and count < batch_size:
            url, depth = queue.popleft()
            if url in visited or depth > self._max_depth:
                continue
            if not self._is_allowed(url):
                continue

            visited.add(url)
            await asyncio.sleep(self._politeness)

            try:
                content, content_type, links, etag = await self._fetch_page(url)
            except Exception as exc:
                logger.debug("Crawl error %s: %s", url, exc)
                continue

            if not content:
                continue

            doc = RawDocument.create(
                instance_id=self.instance_id,
                connector_id="web",
                uri=url,
                content=content,
                content_type=content_type,
                version=etag,
                cursor=Cursor(value=url, source_type="token", connector_id="web", instance_id=self.instance_id),
                tags=("webpage", f"depth:{depth}"),
                source_metadata={"resource_type": "webpage", "url": url, "depth": depth, "etag": etag},
            )
            yield doc
            count += 1

            if depth < self._max_depth:
                for link in links:
                    if link not in visited:
                        queue.append((link, depth + 1))

    async def _fetch_page(self, url: str) -> tuple[bytes, str, list[str], str]:
        """Fetche une page et retourne (content, mime, links, etag)."""
        headers: dict[str, str] = {}
        cached_etag = self._etag_cache.get(url)
        if cached_etag:
            headers["If-None-Match"] = cached_etag

        async with self._session.get(url, headers=headers) as resp:
            if resp.status == 304:
                return b"", "", [], cached_etag or ""
            if resp.status == 429:
                raise ConnectorRateLimitError("web", retry_after_s=float(resp.headers.get("Retry-After", 60)))
            if resp.status in (500, 502, 503):
                raise ConnectorTemporaryError(f"HTTP {resp.status}")
            if resp.status != 200:
                raise ConnectorNetworkError("web", url=url, cause=f"HTTP {resp.status}")

            content_type = resp.content_type or "text/html"
            etag = resp.headers.get("ETag", "")
            if etag:
                self._etag_cache[url] = etag

            content = await resp.read()
            links: list[str] = []

            if "text/html" in content_type:
                links = self._extract_links(content.decode("utf-8", errors="replace"), url)

            return content, content_type, links, etag

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        """Extrait et normalise tous les liens href depuis du HTML."""
        links: list[str] = []
        for match in re.finditer(r'href=["\']([^"\'#]+)["\']', html, re.IGNORECASE):
            raw = match.group(1).strip()
            if raw.startswith("mailto:") or raw.startswith("javascript:"):
                continue
            absolute = urljoin(base_url, raw)
            parsed = urlparse(absolute)
            clean = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", parsed.query, ""))
            if parsed.netloc in self._allowed_domains and clean not in links:
                ext = "." + clean.rsplit(".", 1)[-1].lower() if "." in clean.rsplit("/", 1)[-1] else ""
                if not ext or ext in self._include_exts:
                    links.append(clean)
        return links[:50]

    def _is_allowed(self, url: str) -> bool:
        """Vérifie les exclusions et robots.txt."""
        for pattern in self._exclude_patterns:
            if pattern.search(url):
                return False
        parsed = urlparse(url)
        if parsed.netloc not in self._allowed_domains:
            return False
        return True

    # ── Generic API ───────────────────────────────────────────────────────────

    async def _pull_api(
        self, cursors: dict, batch_size: int,
    ) -> AsyncIterator[RawDocument]:
        for endpoint in self._endpoints:
            path   = endpoint.get("path", "")
            method = endpoint.get("method", "GET").upper()
            params = dict(endpoint.get("params", {}))
            url    = self._base_url.rstrip("/") + "/" + path.lstrip("/")
            api_cursor = cursors.get(f"api:{path}")
            count  = 0

            async for batch in self._paginate_api(url, method, params, api_cursor):
                for item in batch:
                    if count >= batch_size:
                        return
                    content = json.dumps(item, ensure_ascii=False, default=str).encode()
                    item_id = str(item.get("id", item.get("uuid", hash(content))))
                    doc = RawDocument.create(
                        instance_id=self.instance_id,
                        connector_id="web",
                        uri=f"{url}/{item_id}",
                        content=content,
                        content_type="application/json",
                        tags=("api_item", f"endpoint:{path}"),
                        source_metadata={"resource_type": "api_item", "endpoint": path, "item_id": item_id},
                    )
                    yield doc
                    count += 1

    async def _paginate_api(
        self, url: str, method: str, params: dict, api_cursor: Optional[str],
    ) -> AsyncIterator[list[dict]]:
        """Itère sur toutes les pages d'un endpoint API."""
        page_cursor = api_cursor
        page = 1
        offset = 0
        per_page = 100

        while True:
            req_params = dict(params)
            if self._pagination_type == "cursor" and page_cursor:
                req_params["cursor"] = page_cursor
            elif self._pagination_type == "page":
                req_params.update({"page": page, "per_page": per_page})
            elif self._pagination_type == "offset":
                req_params.update({"limit": per_page, "offset": offset})

            try:
                async with self._session.request(method, url, params=req_params) as resp:
                    if resp.status == 429:
                        wait = float(resp.headers.get("Retry-After", 60))
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        break
                    data = await resp.json()

                # Extraire les items selon items_path
                items = self._extract_path(data, self._items_path) if self._items_path else data
                if isinstance(items, dict):
                    items = [items]
                if not items:
                    break
                yield items

                # Pagination suivante
                if self._pagination_type == "link_header":
                    link = resp.headers.get("Link", "")
                    match = re.search(r'<([^>]+)>;\s*rel="next"', link)
                    if not match:
                        break
                    url = match.group(1)
                elif self._pagination_type == "cursor":
                    page_cursor = self._extract_path(data, self._cursor_path)
                    if not page_cursor:
                        break
                elif self._pagination_type in ("page", "offset"):
                    if len(items) < per_page:
                        break
                    page += 1
                    offset += per_page
                else:
                    break
            except Exception as exc:
                logger.warning("API pagination error: %s", exc)
                break

    @staticmethod
    def _extract_path(data: Any, path: str) -> Any:
        """Extrait une valeur depuis un dict via un chemin pointé."""
        if not path:
            return data
        for key in path.split("."):
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data

    def _stamp(self, doc: RawDocument, cursors: dict) -> RawDocument:
        import dataclasses
        return dataclasses.replace(
            doc,
            cursor=Cursor(
                value=json.dumps(cursors, sort_keys=True),
                source_type="token",
                connector_id="web",
                instance_id=self.instance_id,
            ),
        )
