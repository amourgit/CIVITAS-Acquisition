"""NotionClient — client HTTP async pour l'API Notion v1."""
from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Optional
import aiohttp
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorAuthenticationError, ConnectorRateLimitError,
    ConnectorTemporaryError, ConnectorFatalError, ConnectorNetworkError,
)

logger = logging.getLogger(__name__)
BASE_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


class NotionClient:
    def __init__(self, token: str, timeout_s: float = 30.0) -> None:
        self._token = token
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None

    async def open(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self): await self.open(); return self
    async def __aexit__(self, *_): await self.close()

    async def get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}{path}"
        assert self._session
        async with self._session.get(url, params=params) as resp:
            return await self._handle(resp, url)

    async def post(self, path: str, body: dict) -> dict[str, Any]:
        url = f"{BASE_URL}{path}"
        assert self._session
        async with self._session.post(url, json=body) as resp:
            return await self._handle(resp, url)

    async def paginate(
        self, path: str, body: dict | None = None, page_size: int = 100,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Itère sur toutes les pages de résultats Notion (cursor-based).
        POST pour search/query, GET pour blocks children.
        """
        cursor: Optional[str] = None
        while True:
            request_body = {**(body or {}), "page_size": page_size}
            if cursor:
                request_body["start_cursor"] = cursor
            data = await self.post(path, request_body)
            results = data.get("results", [])
            if results:
                yield results
            if not data.get("has_more", False):
                break
            cursor = data.get("next_cursor")

    async def get_children(
        self, block_id: str, page_size: int = 100,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Itère sur les blocs enfants (GET avec cursor dans params)."""
        cursor: Optional[str] = None
        while True:
            params: dict = {"page_size": page_size}
            if cursor:
                params["start_cursor"] = cursor
            data = await self.get(f"/blocks/{block_id}/children", params=params)
            results = data.get("results", [])
            if results:
                yield results
            if not data.get("has_more", False):
                break
            cursor = data.get("next_cursor")

    async def _handle(self, resp: aiohttp.ClientResponse, url: str) -> dict[str, Any]:
        if resp.status == 200:
            return await resp.json()
        body = {}
        try:
            body = await resp.json()
        except Exception:
            pass
        msg = body.get("message", "")
        code = body.get("code", "")

        if resp.status == 401:
            raise ConnectorAuthenticationError("notion", f"401: {msg}")
        if resp.status == 403:
            raise ConnectorAuthenticationError("notion", f"403 Forbidden: {msg}")
        if resp.status == 404:
            raise ResourceNotFoundError(url)
        if resp.status == 429:
            retry = float(resp.headers.get("Retry-After", 60))
            raise ConnectorRateLimitError("notion", retry_after_s=retry)
        if resp.status in (500, 502, 503):
            raise ConnectorTemporaryError(f"{resp.status} Notion error: {msg}")
        raise ConnectorNetworkError("notion", url=url, cause=f"HTTP {resp.status}: {msg}")


class ResourceNotFoundError(Exception):
    def __init__(self, url: str):
        super().__init__(f"Not found: {url}")
        self.url = url
