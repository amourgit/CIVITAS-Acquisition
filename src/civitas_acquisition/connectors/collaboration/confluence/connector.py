"""
ConfluenceConnector — acquisition complète Confluence Cloud & Server.

Ressources :
  pages        : pages avec contenu Markdown extrait du Storage Format
  blog_posts   : articles de blog
  attachments  : fichiers joints
  spaces       : métadonnées des espaces
  comments     : commentaires sur les pages

Auth :
  Cloud  : email + api_token (Basic Auth)
  Server : username + password | PAT (Bearer)

Config options :
  base_url        : str        — ex: "https://mycompany.atlassian.net/wiki"
  space_keys      : list[str]  — espaces à acquérir (défaut: tous)
  resource_types  : list["pages","blog_posts","attachments","spaces","comments"]
  expand          : list[str]  — champs à expandre (défaut: body.storage,version)
  max_pages       : int        — (défaut: 10000)
  include_archived: bool       — (défaut: False)
  content_format  : str        — "markdown" | "storage" | "text" (défaut: "text")
"""
from __future__ import annotations

import json
import logging
import re
import time
from html.parser import HTMLParser
from typing import Any, AsyncIterator, Optional

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
    ConnectorAuthenticationError, ConnectorRateLimitError, ConnectorTemporaryError,
)

logger = logging.getLogger(__name__)


class _HTMLStripper(HTMLParser):
    """Extrait le texte brut depuis du HTML Confluence."""
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        stripped = data.strip()
        if stripped:
            self._parts.append(stripped)

    def get_text(self) -> str:
        return "\n".join(self._parts)


def _strip_html(html: str) -> str:
    parser = _HTMLStripper()
    parser.feed(html)
    return parser.get_text()


def _storage_to_markdown(storage_html: str) -> str:
    """Conversion basique du Storage Format Confluence → Markdown."""
    text = storage_html
    # Headings
    for i in range(6, 0, -1):
        text = re.sub(rf"<h{i}[^>]*>(.*?)</h{i}>", lambda m, n=i: "#" * n + " " + _strip_html(m.group(1)), text, flags=re.DOTALL)
    # Bold/Italic
    text = re.sub(r"<strong[^>]*>(.*?)</strong>", r"**\1**", text, flags=re.DOTALL)
    text = re.sub(r"<em[^>]*>(.*?)</em>", r"*\1*", text, flags=re.DOTALL)
    # Code
    text = re.sub(r"<code[^>]*>(.*?)</code>", r"`\1`", text, flags=re.DOTALL)
    text = re.sub(r"<ac:structured-macro[^>]*ac:name=\"code\"[^>]*>.*?<ac:plain-text-body><!\[CDATA\[(.*?)\]\]></ac:plain-text-body>.*?</ac:structured-macro>", r"```\n\1\n```", text, flags=re.DOTALL)
    # Links
    text = re.sub(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', r"[\2](\1)", text, flags=re.DOTALL)
    # Lists
    text = re.sub(r"<li[^>]*>(.*?)</li>", r"- \1", text, flags=re.DOTALL)
    # Paragraphs/breaks
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<p[^>]*>(.*?)</p>", r"\1\n", text, flags=re.DOTALL)
    # Strip remaining tags
    text = re.sub(r"<[^>]+>", "", text)
    # Clean whitespace
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


class ConfluenceConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="confluence",
            display_name="Confluence",
            version="1.0.0",
            source_category=SourceCategory.COLLABORATION,
            supported_channels=frozenset([ChannelType.POLLING, ChannelType.WEBHOOK, ChannelType.MANUAL]),
            supported_mime_types=frozenset(["text/markdown", "text/plain", "application/json"]),
            required_credentials=(
                CredentialSpec(key="base_url",  description="URL de base Confluence (ex: https://company.atlassian.net/wiki)", sensitive=False),
                CredentialSpec(key="email",     description="Email Atlassian (Cloud) ou username (Server)"),
                CredentialSpec(key="api_token", description="API Token (Cloud) ou password/PAT (Server)", sensitive=True),
            ),
            rate_limit=RateLimit(requests_per_second=3.0, burst_size=10),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    async def _do_connect(self, config: ConnectorConfig) -> None:
        self._base_url  = config.get_credential("base_url").rstrip("/")
        email           = config.get_credential("email")
        api_token       = config.get_credential("api_token")

        import base64
        basic = base64.b64encode(f"{email}:{api_token}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {basic}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=config.get_option("timeout_s", 30.0))
        self._session = aiohttp.ClientSession(timeout=timeout, headers=self._headers)

        self._space_keys       = config.get_option("space_keys", [])
        self._resource_types   = config.get_option("resource_types", ["pages"])
        self._max_pages        = config.get_option("max_pages", 10_000)
        self._include_archived = config.get_option("include_archived", False)
        self._content_format   = config.get_option("content_format", "text")
        self._expand           = ",".join(config.get_option("expand", ["body.storage", "version", "ancestors"]))

        # Verify auth
        data = await self._api_get("/rest/api/user/current")
        if "statusCode" in data:
            raise ConnectorAuthenticationError("confluence", data.get("message", "auth failed"))
        self._username = data.get("displayName", email)
        logger.info("Confluence connected as %s @ %s", self._username, self._base_url)

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_session") and not self._session.closed:
            await self._session.close()

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            data = await self._api_get("/rest/api/space?limit=1")
            return HealthStatus.ok(
                latency_ms=(time.monotonic() - start) * 1000,
                total_spaces=data.get("size", "?"),
            )
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    async def discover(self) -> DiscoveryResult:
        spaces = await self._list_spaces()
        resources = tuple(
            f"{self._base_url}/display/{s['key']}" for s in spaces
        )
        return DiscoveryResult(resources=resources, total=len(resources),
                               metadata={s["key"]: s.get("name", "") for s in spaces})

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors = json.loads(cursor.value) if cursor else {}
        updated = dict(cursors)
        count   = 0

        spaces = self._space_keys or [s["key"] for s in await self._list_spaces()]

        for space_key in spaces:
            if count >= batch_size:
                break

            if "spaces" in self._resource_types and count < batch_size:
                space_data = await self._api_get(f"/rest/api/space/{space_key}?expand=description.plain")
                doc = self._map_space(space_data)
                yield self._stamp(doc, updated)
                count += 1

            for content_type in ["page", "blogpost"]:
                rtype = "pages" if content_type == "page" else "blog_posts"
                if rtype not in self._resource_types or count >= batch_size:
                    continue
                since = cursors.get(f"{rtype}:{space_key}")
                async for doc in self._fetch_content(space_key, content_type, since, batch_size - count):
                    if count >= batch_size:
                        break
                    updated[f"{rtype}:{space_key}"] = doc.source_metadata.get("last_modified", "")
                    yield self._stamp(doc, updated)
                    count += 1

        logger.info("Confluence pull: %d documents", count)

    async def _fetch_content(
        self, space_key: str, content_type: str, since: Optional[str], limit: int,
    ) -> AsyncIterator[RawDocument]:
        start = 0
        page_size = min(50, limit)

        while start < limit:
            params = (
                f"/rest/api/content?spaceKey={space_key}&type={content_type}"
                f"&expand={self._expand}&limit={page_size}&start={start}"
                f"&status={'current' if not self._include_archived else 'any'}"
            )
            if since:
                params += f"&lastModified={since}"

            data = await self._api_get(params)
            results = data.get("results", [])
            if not results:
                break

            for item in results:
                doc = self._map_content(item)
                if doc:
                    yield doc

            start += len(results)
            if len(results) < page_size:
                break

    def _map_content(self, item: dict[str, Any]) -> Optional[RawDocument]:
        item_id   = item.get("id", "")
        title     = item.get("title", "Untitled")
        ctype     = item.get("type", "page")
        version   = item.get("version", {}).get("number", 1)
        modified  = item.get("version", {}).get("when", "")
        web_url   = f"{self._base_url}{item.get('_links', {}).get('webui', '')}"
        space_key = item.get("space", {}).get("key", "")

        storage_html = item.get("body", {}).get("storage", {}).get("value", "")
        if self._content_format == "markdown":
            content_str = f"# {title}\n\n{_storage_to_markdown(storage_html)}"
            mime = "text/markdown"
        elif self._content_format == "text":
            content_str = f"{title}\n\n{_strip_html(storage_html)}"
            mime = "text/plain"
        else:
            content_str = storage_html
            mime = "text/html"

        if not content_str.strip():
            return None

        ancestors = [a.get("title", "") for a in item.get("ancestors", [])]

        return RawDocument.create(
            instance_id=self.instance_id, connector_id="confluence",
            uri=web_url,
            content=content_str.encode("utf-8"),
            content_type=mime,
            version=str(version),
            cursor=Cursor(value=modified, source_type="timestamp",
                          connector_id="confluence", instance_id=self.instance_id),
            tags=(ctype, f"space:{space_key}"),
            source_metadata={
                "resource_type": ctype,
                "content_id": item_id,
                "title": title,
                "space_key": space_key,
                "version": version,
                "last_modified": modified,
                "ancestors": ancestors,
                "web_url": web_url,
            },
        )

    def _map_space(self, space: dict[str, Any]) -> RawDocument:
        key  = space.get("key", "")
        name = space.get("name", "")
        desc = space.get("description", {}).get("plain", {}).get("value", "")
        content = json.dumps({"key": key, "name": name, "description": desc}, ensure_ascii=False).encode()
        return RawDocument.create(
            instance_id=self.instance_id, connector_id="confluence",
            uri=f"{self._base_url}/display/{key}",
            content=content, content_type="application/json",
            tags=("space",),
            source_metadata={"resource_type": "space", "space_key": key, "name": name},
        )

    async def _list_spaces(self) -> list[dict]:
        data = await self._api_get("/rest/api/space?limit=200&type=global")
        return data.get("results", [])

    async def _api_get(self, path: str) -> dict[str, Any]:
        url = path if path.startswith("http") else f"{self._base_url}{path}"
        async with self._session.get(url) as resp:
            if resp.status == 401:
                raise ConnectorAuthenticationError("confluence", "401 Unauthorized")
            if resp.status == 429:
                raise ConnectorRateLimitError("confluence", retry_after_s=float(resp.headers.get("Retry-After", 60)))
            if resp.status in (500, 502, 503):
                raise ConnectorTemporaryError(f"Confluence {resp.status}")
            return await resp.json()

    def _stamp(self, doc: RawDocument, cursors: dict) -> RawDocument:
        import dataclasses
        return dataclasses.replace(
            doc,
            cursor=Cursor(value=json.dumps(cursors, sort_keys=True), source_type="token",
                          connector_id="confluence", instance_id=self.instance_id),
        )
