"""
AcquisitionJobRecord — snapshot immuable d'un job d'acquisition.

Distinct de l'entité mutable AcquisitionJob dans domain/.
Ce record est utilisé dans les événements, le monitoring et le repository.

L'AcquisitionJob est l'unité de travail centrale de la plateforme.
Il représente une exécution complète depuis un connecteur source :
  - quel connecteur
  - quel canal a déclenché l'exécution
  - quelle configuration a été appliquée
  - quel curseur de départ
  - combien de documents produits
  - les erreurs éventuelles
  - les métriques de l'exécution

Tous les composants de la plateforme collaborent autour de cette unité
plutôt que de s'appeler directement entre eux.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Optional
from uuid import uuid4

from .connector_manifest import ChannelType


class JobStatus(Enum):
    """Cycle de vie d'un AcquisitionJob."""
    PENDING   = auto()   # Créé, en attente d'exécution
    RUNNING   = auto()   # En cours d'exécution
    COMPLETED = auto()   # Terminé avec succès
    FAILED    = auto()   # Terminé avec erreur critique
    CANCELLED = auto()   # Annulé avant complétion


class JobTrigger(Enum):
    """Ce qui a déclenché le job."""
    SCHEDULED   = auto()   # Déclenché par le scheduler (polling)
    WEBHOOK     = auto()   # Déclenché par un webhook inbound
    STREAMING   = auto()   # Déclenché par un event stream
    QUEUE       = auto()   # Déclenché par un message de queue
    FILE_EVENT  = auto()   # Déclenché par un changement de fichier
    MANUAL      = auto()   # Déclenché manuellement par un opérateur


@dataclass(frozen=True)
class AcquisitionJobRecord:
    """
    Snapshot immuable d'un AcquisitionJob à un instant T.
    Utilisé dans les événements, le monitoring et la persistence.
    """

    job_id: str
    connector_id: str
    instance_id: str
    channel_type: ChannelType
    trigger: JobTrigger
    status: JobStatus
    created_at: datetime

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Curseurs
    starting_cursor_value: Optional[str] = None
    final_cursor_value: Optional[str] = None

    # Compteurs
    documents_acquired: int = 0
    documents_skipped: int = 0
    documents_failed: int = 0

    # Multi-tenant
    workspace_id: Optional[str] = None

    # Métadonnées opérationnelles libres
    metadata: dict[str, Any] = field(default_factory=dict)

    # Erreur finale si FAILED
    failure_reason: Optional[str] = None

    @property
    def duration_ms(self) -> Optional[float]:
        """Durée en ms. None si pas encore terminé."""
        if self.started_at is None or self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds() * 1000

    @property
    def total_documents(self) -> int:
        return self.documents_acquired + self.documents_skipped + self.documents_failed

    @property
    def success_rate(self) -> Optional[float]:
        """Taux de succès. None si aucun document traité."""
        total = self.total_documents
        if total == 0:
            return None
        return self.documents_acquired / total

    def is_terminal(self) -> bool:
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

    def __repr__(self) -> str:
        return (
            f"AcquisitionJobRecord("
            f"job_id={self.job_id[:8]}..., "
            f"connector={self.connector_id!r}, "
            f"status={self.status.name}, "
            f"acquired={self.documents_acquired})"
        )


def new_job_id() -> str:
    return str(uuid4())
