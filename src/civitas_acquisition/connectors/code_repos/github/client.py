"""
GitHubClient — client HTTP async pour l'API GitHub REST v3 + GraphQL v4.

Améliorations vs v1 :
  - graphql() : support GraphQL avec variables
  - get_installation_repos() : endpoint spécifique GitHub App
  - Retry automatique 502/503/504 (3 tentatives, 1s entre chaque)
  - Meilleure extraction du next_url depuis Link header
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

BASE_URL        = "https://api.github.com"
GRAPHQL_URL     = "https://api.github.com/graphql"
GITHUB_VERSION  = "2022-11-28"

_DEFAULT_HEADERS = {
    "Accept":             "application/vnd.github+json",
    "X-GitHub-Api-Version": GITHUB_VERSION,
    "User-Agent":         "CIVITAS-Acquisition/1.0",
}

_SERVER_ERRORS  = (500, 502, 503, 504)
_MAX_SERVER_RETRIES = 3


class ResourceNotFoundError(Exception):
    def __init__(self, url: str) -> None:
        super().__init__(f"Resource not found: {url}")
        self.url = url


class GitHubClient:
    """
    Client HTTP async pour l'API GitHub REST v3 et GraphQL v4.

    Usage :
        async with GitHubClient(auth) as client:
            user = await client.get("/user")
            repos = await client.collect_all("/user/repos")
            result = await client.graphql("query { viewer { login } }")
    """

    def __init__(self, auth: GitHubAuth, timeout_s: float = 30.0) -> None:
        self._auth    = auth
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit: Optional[RateLimitInfo] = None
        self._etag_cache: dict[str, tuple[str, Any]] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def open(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers=_DEFAULT_HEADERS,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> GitHubClient:
        await self.open(); return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ── Core GET ──────────────────────────────────────────────────────────────

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        use_etag: bool = False,
    ) -> Any:
        url = path if path.startswith("https://") else f"{BASE_URL}{path}"
        token   = await self._auth.get_token()
        headers = {"Authorization": self._auth.auth_header(token)}

        if use_etag and url in self._etag_cache:
            headers["If-None-Match"] = self._etag_cache[url][0]

        await self._wait_rate_limit()
        assert self._session

        for attempt in range(1, _MAX_SERVER_RETRIES + 1):
            async with self._session.get(url, params=params, headers=headers) as resp:
                self._update_rate_limit(dict(resp.headers))
                if resp.status == 304:
                    return self._etag_cache[url][1]
                if resp.status == 200:
                    data = await resp.json()
                    if use_etag and "ETag" in resp.headers:
                        self._etag_cache[url] = (resp.headers["ETag"], data)
                    return data
                if resp.status in _SERVER_ERRORS and attempt < _MAX_SERVER_RETRIES:
                    await asyncio.sleep(attempt)
                    continue
                await self._handle_error(resp, url)

    async def get_raw(self, url: str) -> bytes:
        token   = await self._auth.get_token()
        headers = {
            "Authorization": self._auth.auth_header(token),
            "Accept":        "application/vnd.github.raw",
        }
        await self._wait_rate_limit()
        assert self._session
        async with self._session.get(url, headers=headers) as resp:
            self._update_rate_limit(dict(resp.headers))
            if resp.status == 200:
                return await resp.read()
            await self._handle_error(resp, url)

    # ── POST / PATCH / DELETE ─────────────────────────────────────────────────

    async def post(self, path: str, body: dict | None = None) -> Any:
        return await self._request("POST", path, body=body)

    async def patch(self, path: str, body: dict | None = None) -> Any:
        return await self._request("PATCH", path, body=body)

    async def delete(self, path: str) -> None:
        await self._request("DELETE", path, expect_body=False)

    async def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        expect_body: bool = True,
    ) -> Any:
        url     = path if path.startswith("https://") else f"{BASE_URL}{path}"
        token   = await self._auth.get_token()
        headers = {"Authorization": self._auth.auth_header(token)}
        assert self._session
        async with self._session.request(method, url, json=body, headers=headers) as resp:
            self._update_rate_limit(dict(resp.headers))
            if resp.status in (200, 201, 204):
                if expect_body and resp.status != 204:
                    return await resp.json()
                return None
            await self._handle_error(resp, url)

    # ── Pagination ────────────────────────────────────────────────────────────

    async def paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        per_page: int = 100,
        max_pages: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        url           = path if path.startswith("https://") else f"{BASE_URL}{path}"
        request_params = {"per_page": per_page, **(params or {})}
        page_count    = 0
        assert self._session

        while url and page_count < max_pages:
            token   = await self._auth.get_token()
            headers = {"Authorization": self._auth.auth_header(token)}
            await self._wait_rate_limit()

            async with self._session.get(url, params=request_params, headers=headers) as resp:
                self._update_rate_limit(dict(resp.headers))
                if resp.status == 200:
                    data = await resp.json()
                    yield data if isinstance(data, list) else [data]
                    page_count  += 1
                    link = resp.headers.get("Link", "")
                    m = _LINK_NEXT_RE.search(link)
                    url           = m.group(1) if m else ""
                    request_params = {}
                else:
                    await self._handle_error(resp, url)
                    break

    async def collect_all(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        async for page in self.paginate(path, params=params, per_page=per_page):
            result.extend(page)
        return result

    # ── GraphQL v4 ────────────────────────────────────────────────────────────

    async def graphql(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute une requête GraphQL GitHub v4.
        Lève ConnectorFatalError si la réponse contient des erreurs.

        Usage :
            result = await client.graphql(
                "query($login: String!) { user(login: $login) { name } }",
                variables={"login": "octocat"},
            )
        """
        token   = await self._auth.get_token()
        headers = {
            "Authorization":    self._auth.auth_header(token),
            "Content-Type":     "application/json",
        }
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        assert self._session
        await self._wait_rate_limit()
        async with self._session.post(GRAPHQL_URL, json=payload, headers=headers) as resp:
            self._update_rate_limit(dict(resp.headers))
            if resp.status != 200:
                await self._handle_error(resp, GRAPHQL_URL)
            data = await resp.json()
            if "errors" in data:
                errors = data["errors"]
                msg = "; ".join(e.get("message", "") for e in errors)
                raise ConnectorFatalError(f"GraphQL errors: {msg}")
            return data.get("data", {})

    # ── Installation repos (GitHub App) ───────────────────────────────────────

    async def get_installation_repos(self) -> list[dict[str, Any]]:
        """
        Liste les repos accessibles par l'installation GitHub App.
        Endpoint spécifique : /installation/repositories
        (Activepieces pattern : getInstallationRepos)
        """
        all_repos: list[dict[str, Any]] = []
        async for page in self.paginate("/installation/repositories", per_page=100):
            # L'API retourne {"total_count": N, "repositories": [...]}
            # mais avec paginate() on reçoit soit une list soit un dict
            if isinstance(page, list):
                # Cas où le premier item est le wrapper
                for item in page:
                    if isinstance(item, dict) and "repositories" in item:
                        all_repos.extend(item["repositories"])
                    elif isinstance(item, dict) and "full_name" in item:
                        all_repos.append(item)
        return all_repos

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
                    "Rate limit low: %d/%d (resets in %.0fs)",
                    info.remaining, info.limit,
                    max(0, info.reset_at - time.time()),
                )

    async def _wait_rate_limit(self) -> None:
        if self._rate_limit and self._rate_limit.remaining == 0:
            wait_s = max(0, self._rate_limit.reset_at - time.time()) + 1
            logger.warning("Rate limit exhausted — sleeping %.0fs", wait_s)
            await asyncio.sleep(wait_s)

    # ── Error handling ────────────────────────────────────────────────────────

    async def _handle_error(
        self, resp: aiohttp.ClientResponse, url: str
    ) -> None:
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
            raise ConnectorAuthenticationError("github", f"401: {body}")
        if status == 403:
            if "rate limit" in body.lower() or "abuse" in body.lower():
                retry = float(resp.headers.get("Retry-After", 60))
                raise ConnectorRateLimitError("github", retry_after_s=retry)
            raise ConnectorAuthenticationError("github", f"403: {body}")
        if status == 404:
            raise ResourceNotFoundError(url)
        if status == 422:
            raise ConnectorFatalError(f"422 Unprocessable: {body}")
        if status == 429:
            retry = float(resp.headers.get("Retry-After", 60))
            raise ConnectorRateLimitError("github", retry_after_s=retry)
        if status in _SERVER_ERRORS:
            raise ConnectorTemporaryError(f"{status} Server Error at {url}: {body}")
        raise ConnectorNetworkError("github", url=url, cause=f"HTTP {status}: {body}")
