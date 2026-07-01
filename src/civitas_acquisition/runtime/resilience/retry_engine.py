"""
RetryEngine — exécution avec retry et backoff exponentiel.

Utilisé par le pipeline d'acquisition pour les opérations réseau
(écriture dans le repository, appels aux connecteurs).

Classification des erreurs :
  - Retryable (temporaires) : réseau, rate limit, indisponibilité temporaire
  - Fatal (non-retryable)   : auth, validation, configuration

Le RetryEngine ne connaît pas les politiques — il reçoit une RetryPolicy
configurée par la couche policies/.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Awaitable, Callable, TypeVar

from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorNetworkError,
    ConnectorRateLimitError,
    ConnectorTemporaryError,
    ConnectorTimeoutError,
)
from civitas_acquisition.contracts.errors.resilience_errors import MaxRetriesExhaustedError

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Erreurs retryables par défaut — peuvent être surchargées via RetryPolicy
DEFAULT_RETRYABLE: tuple[type[Exception], ...] = (
    ConnectorNetworkError,
    ConnectorRateLimitError,
    ConnectorTemporaryError,
    ConnectorTimeoutError,
    TimeoutError,
    ConnectionError,
    OSError,
)


class RetryEngine:
    """
    Exécute une fonction async avec retry automatique sur erreurs temporaires.

    Usage :
        engine = RetryEngine(RetryPolicy(max_attempts=3))
        result = await engine.execute(my_async_fn, arg1, arg2)
    """

    def __init__(self, policy: "RetryPolicy") -> None:  # noqa: F821
        self._policy = policy

    async def execute(
        self,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Exécute fn(*args, **kwargs) avec retry selon la policy.
        Lève MaxRetriesExhaustedError si toutes les tentatives échouent.
        Relève immédiatement les erreurs fatales sans retry.
        """
        policy = self._policy
        delay_ms = policy.initial_delay_ms
        last_exc: Exception | None = None

        for attempt in range(1, policy.max_attempts + 1):
            try:
                return await fn(*args, **kwargs)

            except ConnectorRateLimitError as exc:
                # Respecter le retry_after de la source si disponible
                if exc.retry_after_s is not None:
                    wait_s = exc.retry_after_s
                else:
                    wait_s = delay_ms / 1000
                logger.warning(
                    "Rate limit hit on attempt %d/%d — waiting %.1fs",
                    attempt, policy.max_attempts, wait_s,
                )
                last_exc = exc
                if attempt < policy.max_attempts:
                    await asyncio.sleep(wait_s)

            except tuple(policy.retryable_errors) as exc:
                last_exc = exc
                if attempt >= policy.max_attempts:
                    break
                wait_ms = delay_ms
                if policy.jitter:
                    wait_ms = random.uniform(delay_ms * 0.5, delay_ms * 1.5)
                logger.warning(
                    "Retryable error on attempt %d/%d (%s) — retrying in %.0fms",
                    attempt, policy.max_attempts, type(exc).__name__, wait_ms,
                )
                await asyncio.sleep(wait_ms / 1000)
                delay_ms = min(
                    delay_ms * policy.backoff_multiplier,
                    policy.max_delay_ms,
                )

            except Exception:
                # Erreur fatale — ne pas retenter
                raise

        raise MaxRetriesExhaustedError(
            attempts=policy.max_attempts,
            last_error=str(last_exc) if last_exc else "",
        ) from last_exc
