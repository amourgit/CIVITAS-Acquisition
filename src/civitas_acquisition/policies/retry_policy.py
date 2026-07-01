"""
RetryPolicy — politiques de retry configurables.

Les politiques sont définies une fois dans la couche policies/ 
et injectées dans le RetryEngine. Aucune politique n'est hardcodée
dans les connecteurs ou le pipeline.

Politiques prédéfinies :
  - AGGRESSIVE : 5 tentatives, backoff court (pour les webhooks urgents)
  - STANDARD   : 3 tentatives, backoff modéré (par défaut)
  - CONSERVATIVE : 2 tentatives, backoff long (pour les sources fragiles)
  - NO_RETRY   : 1 seule tentative (pour les opérations idempotentes critiques)
"""

from __future__ import annotations

from dataclasses import dataclass, field

from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorNetworkError,
    ConnectorRateLimitError,
    ConnectorTemporaryError,
    ConnectorTimeoutError,
)


@dataclass(frozen=True)
class RetryPolicy:
    """
    Politique de retry injectable dans le RetryEngine.
    
    Tous les champs sont immuables — une policy est créée une fois,
    partagée entre plusieurs instances de RetryEngine.
    """
    max_attempts: int = 3
    initial_delay_ms: float = 1_000.0
    max_delay_ms: float = 60_000.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retryable_errors: tuple[type[Exception], ...] = field(
        default_factory=lambda: (
            ConnectorNetworkError,
            ConnectorRateLimitError,
            ConnectorTemporaryError,
            ConnectorTimeoutError,
            TimeoutError,
            ConnectionError,
            OSError,
        )
    )

    def with_max_attempts(self, n: int) -> RetryPolicy:
        """Retourne une copie avec max_attempts modifié."""
        return RetryPolicy(
            max_attempts=n,
            initial_delay_ms=self.initial_delay_ms,
            max_delay_ms=self.max_delay_ms,
            backoff_multiplier=self.backoff_multiplier,
            jitter=self.jitter,
            retryable_errors=self.retryable_errors,
        )


# ── Politiques prédéfinies ────────────────────────────────────────────────────

AGGRESSIVE = RetryPolicy(
    max_attempts=5,
    initial_delay_ms=200.0,
    max_delay_ms=10_000.0,
    backoff_multiplier=1.5,
    jitter=True,
)
"""5 tentatives, backoff agressif. Pour les webhooks temps-réel."""

STANDARD = RetryPolicy(
    max_attempts=3,
    initial_delay_ms=1_000.0,
    max_delay_ms=60_000.0,
    backoff_multiplier=2.0,
    jitter=True,
)
"""3 tentatives, backoff modéré. Politique par défaut."""

CONSERVATIVE = RetryPolicy(
    max_attempts=2,
    initial_delay_ms=5_000.0,
    max_delay_ms=300_000.0,
    backoff_multiplier=3.0,
    jitter=True,
)
"""2 tentatives, backoff long. Pour les sources fragiles ou rate-limitées."""

NO_RETRY = RetryPolicy(
    max_attempts=1,
    initial_delay_ms=0.0,
    max_delay_ms=0.0,
    backoff_multiplier=1.0,
    jitter=False,
)
"""Pas de retry. Pour les opérations critiques idempotentes."""
