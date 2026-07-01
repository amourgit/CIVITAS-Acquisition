"""
CircuitBreaker — protection contre les cascades de défaillances.

États :
  CLOSED    → fonctionnement normal. Les appels passent.
  OPEN      → circuit ouvert. Les appels sont bloqués immédiatement.
  HALF_OPEN → fenêtre de test. Un nombre limité d'appels passe pour sonder la reprise.

Transitions :
  CLOSED    → OPEN      si failure_count >= failure_threshold dans la fenêtre
  OPEN      → HALF_OPEN après recovery_timeout_s secondes
  HALF_OPEN → CLOSED    si les appels de sonde réussissent
  HALF_OPEN → OPEN      si les appels de sonde échouent
"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum, auto
from typing import Any, Awaitable, Callable, TypeVar

from civitas_acquisition.contracts.errors.resilience_errors import CircuitOpenError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED    = auto()
    OPEN      = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    """
    Implémentation du pattern Circuit Breaker.

    Usage :
        cb = CircuitBreaker(
            resource_id="raw-repository",
            failure_threshold=5,
            recovery_timeout_s=60,
            half_open_max_calls=3,
        )
        result = await cb.call(my_async_fn, arg1, arg2)
    """

    def __init__(
        self,
        resource_id: str,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> None:
        self.resource_id = resource_id
        self._failure_threshold = failure_threshold
        self._recovery_timeout_s = recovery_timeout_s
        self._half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count: int = 0
        self._half_open_calls: int = 0
        self._half_open_successes: int = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def call(
        self,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        async with self._lock:
            self._maybe_transition_to_half_open()
            self._guard_open()
            self._half_open_calls += 1 if self._state == CircuitState.HALF_OPEN else 0

        try:
            result = await fn(*args, **kwargs)
            async with self._lock:
                self._on_success()
            return result
        except Exception:
            async with self._lock:
                self._on_failure()
            raise

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and (time.monotonic() - self._opened_at) >= self._recovery_timeout_s
        ):
            self._state = CircuitState.HALF_OPEN
            self._half_open_calls = 0
            self._half_open_successes = 0
            logger.info("Circuit HALF_OPEN for '%s' — probing recovery", self.resource_id)

    def _guard_open(self) -> None:
        if self._state == CircuitState.OPEN:
            raise CircuitOpenError(self.resource_id)
        if (
            self._state == CircuitState.HALF_OPEN
            and self._half_open_calls >= self._half_open_max_calls
        ):
            raise CircuitOpenError(self.resource_id)

    def _on_success(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._half_open_successes += 1
            if self._half_open_successes >= self._half_open_max_calls:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._opened_at = None
                logger.info("Circuit CLOSED for '%s' — recovery confirmed", self.resource_id)
        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0

    def _on_failure(self) -> None:
        self._failure_count += 1
        if self._state == CircuitState.HALF_OPEN:
            self._trip()
            return
        if self._failure_count >= self._failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        logger.warning(
            "Circuit OPEN for '%s' — %d failures. Recovery in %.0fs",
            self.resource_id, self._failure_count, self._recovery_timeout_s,
        )

    def reset(self) -> None:
        """Reset manuel — utile pour les tests ou opérations d'urgence."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = None

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(resource={self.resource_id!r}, "
            f"state={self._state.name}, "
            f"failures={self._failure_count})"
        )
