"""
BackpressurePolicy — contrôle de la pression en entrée du système.

Quand les workers sont surchargés, la file de tâches grossit.
Sans backpressure, la file est non bornée → OOM en production.

Stratégies disponibles :
  - BLOCK   : bloque l'appelant jusqu'à qu'une place se libère (max_wait_s)
  - DROP    : rejette la tâche si la file est pleine (perte intentionnelle)
  - REJECT  : lève BackpressureError immédiatement si plein
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from civitas_acquisition.contracts.errors.base import AcquisitionError


class BackpressureStrategy(Enum):
    BLOCK  = auto()   # Attendre une place libre
    DROP   = auto()   # Abandonner silencieusement si plein
    REJECT = auto()   # Lever une erreur si plein


class BackpressureError(AcquisitionError):
    """Levée quand la file de tâches est pleine et la stratégie est REJECT."""
    def __init__(self, queue_size: int, max_size: int) -> None:
        super().__init__(
            f"Backpressure: task queue is full ({queue_size}/{max_size})",
            context={"queue_size": queue_size, "max_size": max_size},
        )


@dataclass(frozen=True)
class BackpressurePolicy:
    """
    Politique de backpressure pour le WorkerDispatcher.
    
    max_queue_size   : taille maximale de la file de tâches
    strategy         : que faire quand la file est pleine
    max_wait_s       : timeout pour la stratégie BLOCK
    high_watermark   : seuil d'alerte (% de remplissage)
    """
    max_queue_size: int = 1_000
    strategy: BackpressureStrategy = BackpressureStrategy.REJECT
    max_wait_s: float = 30.0
    high_watermark: float = 0.80   # Alerte à 80% de remplissage

    def is_high_watermark(self, current_size: int) -> bool:
        return (current_size / self.max_queue_size) >= self.high_watermark

    def is_full(self, current_size: int) -> bool:
        return current_size >= self.max_queue_size


# ── Politiques prédéfinies ────────────────────────────────────────────────────

WEBHOOK_BACKPRESSURE = BackpressurePolicy(
    max_queue_size=500,
    strategy=BackpressureStrategy.BLOCK,
    max_wait_s=5.0,
    high_watermark=0.70,
)
"""Webhooks : on bloque l'appelant (HTTP) brièvement plutôt que de perdre des events."""

POLLING_BACKPRESSURE = BackpressurePolicy(
    max_queue_size=2_000,
    strategy=BackpressureStrategy.REJECT,
    high_watermark=0.80,
)
"""Polling : on rejette si plein, le scheduler réessaiera au prochain cycle."""

STREAMING_BACKPRESSURE = BackpressurePolicy(
    max_queue_size=10_000,
    strategy=BackpressureStrategy.DROP,
    high_watermark=0.90,
)
"""Streaming : on peut se permettre de dropper des messages — le consumer group gère le lag."""
