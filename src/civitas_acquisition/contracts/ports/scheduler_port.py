"""
SchedulerPort — interface abstraite des stratégies de planification.

Découple le canal Polling de la logique de timing.
Implémentations : Cron, AdaptiveBackoff, EventTriggered.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable


@dataclass
class JobResult:
    """Résultat d'un cycle d'exécution du scheduler."""
    documents_found: int = 0
    documents_processed: int = 0
    documents_skipped: int = 0
    documents_failed: int = 0
    duration_ms: float = 0.0

    @property
    def success(self) -> bool:
        return self.documents_failed == 0

    @property
    def has_work(self) -> bool:
        """True si des documents ont été trouvés dans cette exécution."""
        return self.documents_found > 0

    def __repr__(self) -> str:
        return (
            f"JobResult(found={self.documents_found}, "
            f"processed={self.documents_processed}, "
            f"skipped={self.documents_skipped}, "
            f"failed={self.documents_failed}, "
            f"duration={self.duration_ms:.1f}ms)"
        )


class SchedulerPort(ABC):
    """
    Interface abstraite pour les stratégies de planification.

    Usage typique dans un PollingChannel :
        async for result in scheduler.schedule(pull_cycle):
            if result.has_work:
                scheduler.reset()
            else:
                scheduler.backoff()
    """

    @abstractmethod
    def schedule(
        self,
        job: Callable[[], Awaitable[JobResult]],
    ) -> AsyncIterator[JobResult]:
        """
        Planifie et exécute le job en boucle selon la stratégie.
        Yields le résultat de chaque exécution.
        La boucle continue jusqu'à stop() ou annulation de la coroutine.
        """
        ...

    @abstractmethod
    def backoff(self) -> None:
        """
        Signale qu'aucun travail n'a été trouvé.
        Augmente l'intervalle jusqu'à max_interval_s.
        """
        ...

    @abstractmethod
    def reset(self) -> None:
        """
        Signale que du travail a été trouvé.
        Réinitialise l'intervalle à base_interval_s.
        """
        ...

    @abstractmethod
    def current_interval_s(self) -> float:
        """Retourne l'intervalle courant en secondes."""
        ...
