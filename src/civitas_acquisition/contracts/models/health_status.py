"""
HealthStatus — résultat d'un healthcheck de connecteur.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass(frozen=True)
class HealthStatus:
    """
    Résultat immuable d'un healthcheck sur une instance connecteur.
    Produit par ConnectorPort.healthcheck().
    """

    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)
    checked_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    @property
    def degraded(self) -> bool:
        """Sain mais avec latence élevée (> 500ms)."""
        return self.healthy and self.latency_ms is not None and self.latency_ms > 500.0

    @classmethod
    def ok(cls, latency_ms: float, **detail: Any) -> HealthStatus:
        return cls(healthy=True, latency_ms=latency_ms, detail=dict(detail))

    @classmethod
    def fail(cls, error: str, **detail: Any) -> HealthStatus:
        return cls(healthy=False, error=error, detail=dict(detail))

    def __str__(self) -> str:
        if self.healthy:
            lat = f"{self.latency_ms:.1f}ms" if self.latency_ms is not None else "?"
            status = "DEGRADED" if self.degraded else "HEALTHY"
            return f"{status} (latency={lat})"
        return f"UNHEALTHY: {self.error}"
