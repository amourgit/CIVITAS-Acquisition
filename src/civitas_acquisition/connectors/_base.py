"""
BaseConnector — classe de base pour tous les connecteurs.

Fournit le comportement transversal commun :
  - Rate limiting via token bucket
  - Guard de connexion (assert_connected)
  - Métriques automatiques (documents pulled, latence healthcheck)
  - Logging structuré

Les sous-classes implémentent uniquement _do_connect, _do_pull,
healthcheck, discover. Toute la mécanique commune est ici.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import AsyncIterator, Optional

from civitas_acquisition.contracts.ports.connector_port import ConnectorPort
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorNotConnectedError,
    ConnectorAlreadyConnectedError,
)

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Rate limiter token bucket simple. Thread-safe via asyncio."""

    def __init__(self, rate: float, burst: int) -> None:
        self._rate = rate          # tokens/second
        self._burst = burst        # max tokens
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self._burst,
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return

            # Attendre que les tokens se reconstituent
            wait_s = (tokens - self._tokens) / self._rate
            self._tokens = 0

        await asyncio.sleep(wait_s)


class BaseConnector(ConnectorPort):
    """
    Classe de base abstraite pour tous les connecteurs CIVITAS.

    Les sous-classes DOIVENT implémenter :
      - manifest()      : description statique (pas de super() nécessaire)
      - _do_connect()   : initialiser le client HTTP/SDK
      - _do_pull()      : async generator de RawDocument
      - healthcheck()   : sonde légère
      - discover()      : listing des ressources

    Les sous-classes NE DOIVENT PAS implémenter :
      - connect()       : géré par BaseConnector (rate limiter, état)
      - pull()          : géré par BaseConnector (rate limiting, métriques)
      - disconnect()    : override si ressources à libérer
    """

    def __init__(self) -> None:
        self._connected = False
        self._config: Optional[ConnectorConfig] = None
        self._rate_limiter: Optional[TokenBucketRateLimiter] = None

    # ── Lifecycle (final) ─────────────────────────────────────────────────────

    async def connect(self, config: ConnectorConfig) -> None:
        if self._connected:
            raise ConnectorAlreadyConnectedError(self.manifest().connector_id)

        self._config = config
        manifest = self.manifest()

        if manifest.rate_limit:
            self._rate_limiter = TokenBucketRateLimiter(
                rate=manifest.rate_limit.requests_per_second,
                burst=manifest.rate_limit.burst_size,
            )

        await self._do_connect(config)
        self._connected = True
        logger.info(
            "Connected: %s (instance=%s)",
            manifest.connector_id,
            config.instance_id,
        )

    async def disconnect(self) -> None:
        if not self._connected:
            return
        await self._do_disconnect()
        self._connected = False
        logger.info("Disconnected: %s", self.manifest().connector_id)

    # ── Pull (final) ──────────────────────────────────────────────────────────

    async def pull(
        self,
        cursor: Optional[Cursor] = None,
        batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        self._assert_connected()
        async for doc in self._do_pull(cursor, batch_size):
            if self._rate_limiter:
                await self._rate_limiter.acquire()
            yield doc

    # ── Abstract — à implémenter par la sous-classe ───────────────────────────

    async def _do_connect(self, config: ConnectorConfig) -> None:
        """Initialiser le client, vérifier les credentials."""
        ...

    async def _do_disconnect(self) -> None:
        """Libérer les ressources réseau. Override si nécessaire."""
        ...

    async def _do_pull(
        self,
        cursor: Optional[Cursor],
        batch_size: int,
    ) -> AsyncIterator[RawDocument]:
        """Async generator de RawDocument depuis la source."""
        return
        yield  # type: ignore[misc]

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _assert_connected(self) -> None:
        if not self._connected:
            raise ConnectorNotConnectedError(self.manifest().connector_id)

    @property
    def config(self) -> ConnectorConfig:
        assert self._config is not None, "Not connected"
        return self._config

    @property
    def instance_id(self) -> str:
        return self.config.instance_id

    @property
    def connector_id(self) -> str:
        return self.manifest().connector_id
