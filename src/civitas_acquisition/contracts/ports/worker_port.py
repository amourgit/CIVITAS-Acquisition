"""
WorkerPort & DispatcherPort — interfaces abstraites du runtime de workers.

Les Channels ne doivent JAMAIS exécuter directement les connecteurs.
Ils publient une tâche. Les Workers l'exécutent.

Ce découplage permet :
  - Contrôle de la concurrence (worker pool)
  - Priorité des tâches (haute priorité pour les webhooks)
  - Back-pressure (queue bornée, refus en cas de surcharge)
  - Reprise après crash (tâches persistées avant exécution)
  - Observabilité fine (métriques par worker)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any
from uuid import uuid4


class TaskPriority(Enum):
    """Priorité d'exécution d'une tâche."""
    LOW    = auto()
    NORMAL = auto()
    HIGH   = auto()
    URGENT = auto()   # Webhooks et events temps-réel


class TaskStatus(Enum):
    QUEUED     = auto()
    RUNNING    = auto()
    DONE       = auto()
    FAILED     = auto()
    CANCELLED  = auto()


@dataclass
class WorkerTask:
    """
    Tâche d'acquisition publiée par un Channel, exécutée par un Worker.
    Contient tout le contexte nécessaire à l'exécution.
    """

    task_id: str = field(default_factory=lambda: str(uuid4()))
    instance_id: str = ""
    connector_id: str = ""
    channel_type: str = ""
    trigger_payload: dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.QUEUED
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    retry_count: int = 0
    workspace_id: str | None = None

    def __repr__(self) -> str:
        return (
            f"WorkerTask("
            f"task_id={self.task_id[:8]}..., "
            f"connector={self.connector_id!r}, "
            f"priority={self.priority.name}, "
            f"status={self.status.name})"
        )


class WorkerPort(ABC):
    """
    Interface abstraite d'un worker d'acquisition.
    Un worker execute une WorkerTask de bout en bout.
    """

    @abstractmethod
    async def execute(self, task: WorkerTask) -> None:
        """
        Exécute la tâche. Gère le cycle de vie complet :
        connect → pull → process → commit cursor → disconnect.
        Met à jour task.status tout au long.
        """
        ...

    @abstractmethod
    def is_busy(self) -> bool:
        """True si le worker exécute actuellement une tâche."""
        ...

    @abstractmethod
    async def cancel_current(self) -> None:
        """Annule la tâche en cours proprement."""
        ...


class DispatcherPort(ABC):
    """
    Interface abstraite du dispatcher de tâches.
    Reçoit les tâches et les distribue aux workers disponibles.
    Gère la file d'attente, la priorité et le back-pressure.
    """

    @abstractmethod
    async def dispatch(self, task: WorkerTask) -> None:
        """
        Enfile la tâche pour exécution.
        Lève BackpressureError si la file est pleine.
        """
        ...

    @abstractmethod
    async def cancel(self, task_id: str) -> None:
        """Annule une tâche en file d'attente ou en cours."""
        ...

    @abstractmethod
    async def queue_size(self) -> int:
        """Nombre de tâches en attente."""
        ...

    @abstractmethod
    async def active_count(self) -> int:
        """Nombre de tâches en cours d'exécution."""
        ...
