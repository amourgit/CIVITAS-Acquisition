"""
GitHubClient — client HTTP async pour l'API GitHub REST v3.

Responsabilités :
  - Authentification automatique (injecte le token à chaque requête)
  - Rate limit tracking et backoff automatique
  - Pagination via Link header (yields des pages)
  - Conditional requests via ETag / If-None-Match
  - Gestion des erreurs HTTP → exceptions du domaine
  - Retries sur les erreurs temporaires (502, 503, 504)

N'a aucune connaissance des ressources GitHub (repos, issues, etc.).
C'est le travail du Fetcher.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any, AsyncIterator, Optional

import aiohttp

from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth
from civitas_acquisition.connectors.code_repos.github.models import RateLimitInfo
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorAuthenticationError,
    ConnectorNetworkError,
    ConnectorRateLimitError,
    ConnectorTemporaryError,
    ConnectorFatalError,
)

logger = logging.getLogger(__name__)

_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')

BASE_URL = "https://api.github.com"
DEFAULT_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "CIVITAS-Acquisition/1.0",
}


class GitHubClient:
    """
    Client HTTP async pour l'API GitHub REST v3.

    Usage :
        async with GitHubClient(auth) as client:
            repo = await client.get("/repos/owner/repo")
            async for page in client.paginate("/repos/owner/repo/issues"):
                for issue in page:
                    ...
    """

    def __init__(self, auth: GitHubAuth, timeout_s: float = 30.0) -> None:
        self._auth = auth
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit: Optional[RateLimitInfo] = None
        # ETag cache: url → (etag, cached_response)
        self._etag_cache: dict[str, tuple[str, Any]] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def open(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers=DEFAULT_HEADERS,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> GitHubClient:
        await self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ── Core requests ─────────────────────────────────────────────────────────

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        use_etag: bool = False,
    ) -> dict[str, Any] | list[Any] | None:
        """
        GET request vers l'API GitHub.
        Retourne None si 304 Not Modified (ETag hit).
        """
        url = path if path.startswith("https://") else f"{BASE_URL}{path}"
        token = await self._auth.get_token()
        headers = {"Authorization": self._auth.auth_header(token)}

        if use_etag and url in self._etag_cache:
            etag, _ = self._etag_cache[url]
            headers["If-None-Match"] = etag

        await self._wait_if_rate_limited()

        assert self._session is not None, "Client not opened. Use async with or call open()."
        async with self._session.get(url, params=params, headers=headers) as resp:
            self._update_rate_limit(dict(resp.headers))

            if resp.status == 304:
                # Cache hit — retourner la réponse mise en cache
                return self._etag_cache[url][1]

            if resp.status == 200:
                data = await resp.json()
                if use_etag and "ETag" in resp.headers:
                    self._etag_cache[url] = (resp.headers["ETag"], data)
                return data

            await self._handle_error(resp, url)

    async def get_raw(self, url: str) -> bytes:
        """GET raw bytes — pour le contenu des fichiers."""
        token = await self._auth.get_token()
        headers = {
            "Authorization": self._auth.auth_header(token),
            "Accept": "application/vnd.github.raw",
        }
        await self._wait_if_rate_limited()
        assert self._session is not None
        async with self._session.get(url, headers=headers) as resp:
            self._update_rate_limit(dict(resp.headers))
            if resp.status == 200:
                return await resp.read()
            await self._handle_error(resp, url)

    async def paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        per_page: int = 100,
        max_pages: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Itère sur toutes les pages d'un endpoint paginé.
        Yields chaque page (liste d'items).
        Suit les liens `next` dans le header Link.
        """
        url = path if path.startswith("https://") else f"{BASE_URL}{path}"
        request_params = {"per_page": per_page, **(params or {})}
        page_count = 0

        while url and page_count < max_pages:
            token = await self._auth.get_token()
            headers = {"Authorization": self._auth.auth_header(token)}
            await self._wait_if_rate_limited()

            assert self._session is not None
            async with self._session.get(url, params=request_params, headers=headers) as resp:
                self._update_rate_limit(dict(resp.headers))

                if resp.status == 200:
                    data = await resp.json()
                    yield data if isinstance(data, list) else [data]
                    page_count += 1
                    # Chercher le lien suivant
                    link_header = resp.headers.get("Link", "")
                    match = _LINK_NEXT_RE.search(link_header)
                    url = match.group(1) if match else ""
                    request_params = {}   # les params sont déjà dans l'URL de pagination
                else:
                    await self._handle_error(resp, url)
                    break

    async def collect_all(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """Collecte tous les items d'un endpoint paginé en une seule liste."""
        result: list[dict[str, Any]] = []
        async for page in self.paginate(path, params=params, per_page=per_page):
            result.extend(page)
        return result

    # ── Rate limit ────────────────────────────────────────────────────────────

    @property
    def rate_limit(self) -> Optional[RateLimitInfo]:
        return self._rate_limit

    def _update_rate_limit(self, headers: dict[str, str]) -> None:
        info = RateLimitInfo.from_headers(headers)
        if info:
            self._rate_limit = info
            if info.remaining < 100:
                logger.warning(
                    "GitHub rate limit low: %d/%d remaining (resets in %.0fs)",
                    info.remaining, info.limit,
                    max(0, info.reset_at - time.time()),
                )

    async def _wait_if_rate_limited(self) -> None:
        """Attend si on approche la limite de taux."""
        if self._rate_limit and self._rate_limit.remaining == 0:
            wait_s = max(0, self._rate_limit.reset_at - time.time()) + 1
            logger.warning("Rate limit exhausted. Sleeping %.0fs...", wait_s)
            await asyncio.sleep(wait_s)

    # ── Error handling ────────────────────────────────────────────────────────

    async def _handle_error(self, resp: aiohttp.ClientResponse, url: str) -> None:
        body = ""
        try:
            data = await resp.json()
            body = data.get("message", "")
        except Exception:
            try:
                body = await resp.text()
            except Exception:
                pass

        status = resp.status

        if status == 401:
            raise ConnectorAuthenticationError("github", f"401 Unauthorized: {body}")

        if status == 403:
            if "rate limit" in body.lower() or "abuse" in body.lower():
                retry_after = float(resp.headers.get("Retry-After", 60))
                raise ConnectorRateLimitError("github", retry_after_s=retry_after)
            raise ConnectorAuthenticationError("github", f"403 Forbidden: {body}")

        if status == 404:
            # 404 est souvent légitime en acquisition (resource supprimée)
            logger.debug("404 at %s — resource not found, skipping", url)
            raise ResourceNotFoundError(url)

        if status == 422:
            raise ConnectorFatalError(f"422 Unprocessable: {body} [{url}]")

        if status == 429:
            retry_after = float(resp.headers.get("Retry-After", 60))
            raise ConnectorRateLimitError("github", retry_after_s=retry_after)

        if status in (500, 502, 503, 504):
            raise ConnectorTemporaryError(f"{status} Server Error at {url}: {body}")

        raise ConnectorNetworkError("github", url=url, cause=f"HTTP {status}: {body}")


class ResourceNotFoundError(Exception):
    """Ressource introuvable (404). Non-fatale — à ignorer dans le fetcher."""
    def __init__(self, url: str) -> None:
        super().__init__(f"Resource not found: {url}")
        self.url = url
