"""
AcquisitionMetrics — métriques opérationnelles de la plateforme d'acquisition.

Expose des métriques Prometheus pour chaque dimension critique :
  - Documents acquis/skippés/failed par connecteur
  - Latence des pulls (histogramme p50/p95/p99)
  - État des circuit breakers
  - Taille de la DLQ
  - Health des connecteurs actifs

L'implémentation utilise un stub par défaut (dict in-memory) pour ne pas
dépendre de prometheus_client au niveau des contracts.
En production, passer PrometheusMetrics (dans security/vault/ adapters).

Usage :
    metrics = AcquisitionMetrics()
    metrics.documents_acquired.inc(connector_id="github", instance_id="inst-1")
    with metrics.pull_duration.time(connector_id="github"):
        ...
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import Generator


@dataclass
class Counter:
    """Compteur simple. Thread-safe via GIL pour les entiers."""
    _values: dict[tuple, int] = field(default_factory=lambda: defaultdict(int))

    def inc(self, count: int = 1, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        self._values[key] += count

    def get(self, **labels: str) -> int:
        key = tuple(sorted(labels.items()))
        return self._values.get(key, 0)

    def total(self) -> int:
        return sum(self._values.values())


@dataclass
class Histogram:
    """Histogram pour les latences. Stocke les observations brutes (dev/test)."""
    _observations: list[tuple[tuple, float]] = field(default_factory=list)

    def observe(self, value: float, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        self._observations.append((key, value))

    @contextmanager
    def time(self, **labels: str) -> Generator[None, None, None]:
        """Context manager pour mesurer une durée en ms."""
        start = monotonic()
        try:
            yield
        finally:
            elapsed_ms = (monotonic() - start) * 1000
            self.observe(elapsed_ms, **labels)


class AcquisitionMetrics:
    """
    Façade unique pour toutes les métriques de la plateforme d'acquisition.

    Chaque composant reçoit une instance de cette classe via injection.
    Jamais d'import direct de prometheus_client dans les composants.
    """

    def __init__(self) -> None:
        # ── Documents ─────────────────────────────────────────────────────────
        self.documents_acquired  = Counter()   # labels: connector_id, instance_id
        self.documents_skipped   = Counter()
        self.documents_failed    = Counter()

        # ── Pipeline ──────────────────────────────────────────────────────────
        self.validation_errors   = Counter()
        self.dedup_hits          = Counter()
        self.dlq_enqueued        = Counter()

        # ── Connectivity ──────────────────────────────────────────────────────
        self.connector_healthy   = Counter()   # 1=healthy, 0=unhealthy
        self.circuit_open        = Counter()   # nombre de fois ouvert

        # ── Retry ─────────────────────────────────────────────────────────────
        self.retry_attempts      = Counter()
        self.retry_exhausted     = Counter()

        # ── Latences ──────────────────────────────────────────────────────────
        self.pull_duration_ms    = Histogram()  # labels: connector_id
        self.healthcheck_ms      = Histogram()
        self.repository_write_ms = Histogram()

        # ── Jobs ──────────────────────────────────────────────────────────────
        self.jobs_started    = Counter()
        self.jobs_completed  = Counter()
        self.jobs_failed     = Counter()
        self.jobs_cancelled  = Counter()

    def snapshot(self) -> dict:
        """Retourne un snapshot complet pour le dashboard ou les tests."""
        return {
            "documents_acquired_total": self.documents_acquired.total(),
            "documents_skipped_total":  self.documents_skipped.total(),
            "documents_failed_total":   self.documents_failed.total(),
            "dlq_enqueued_total":       self.dlq_enqueued.total(),
            "jobs_completed_total":     self.jobs_completed.total(),
            "jobs_failed_total":        self.jobs_failed.total(),
            "retry_attempts_total":     self.retry_attempts.total(),
        }
