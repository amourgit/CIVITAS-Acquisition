"""
ExecutionEnginePort — interface abstraite du moteur d'exécution.

L'Execution Engine est le chef d'orchestre de la plateforme.
Il reçoit une demande d'acquisition et sélectionne le runner adapté
selon le type de canal :

  Webhook   → ImmediateRunner   (exécution synchrone, réponse HTTP attendue)
  Polling   → CronRunner        (exécution planifiée par le scheduler adaptatif)
  Streaming → LongRunningRunner (worker long-lived, écoute continue)
  Queue     → QueueRunner       (consumer de messages, ack/nack)
  FileDrop  → WatchRunner       (surveillance de répertoire)
  Manual    → ImmediateRunner   (exécution one-shot, résultat attendu)

Le Scheduler devient une stratégie du PollingRunner, pas un composant central.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models.acquisition_job import AcquisitionJobRecord, JobTrigger
from ..models.connector_manifest import ChannelType


class ExecutionEnginePort(ABC):
    """
    Interface abstraite du moteur d'exécution.

    Reçoit des demandes d'acquisition et les route vers le runner approprié.
    Gère le cycle de vie complet d'un AcquisitionJob.
    """

    @abstractmethod
    async def submit(
        self,
        instance_id: str,
        trigger: JobTrigger,
        payload: dict | None = None,
    ) -> AcquisitionJobRecord:
        """
        Soumet une demande d'acquisition.
        Crée un AcquisitionJob, sélectionne le runner, et démarre l'exécution.
        Retourne le record initial (status=RUNNING).
        """
        ...

    @abstractmethod
    async def cancel(self, job_id: str) -> None:
        """Annule un job en cours."""
        ...

    @abstractmethod
    async def get_job(self, job_id: str) -> AcquisitionJobRecord | None:
        """Récupère l'état actuel d'un job."""
        ...

    @abstractmethod
    async def list_active_jobs(self) -> list[AcquisitionJobRecord]:
        """Retourne tous les jobs en cours d'exécution."""
        ...


class RunnerPort(ABC):
    """
    Interface abstraite d'un runner d'exécution.
    Chaque type de canal a son propre runner.
    """

    @abstractmethod
    def channel_type(self) -> ChannelType:
        """Le type de canal que ce runner gère."""
        ...

    @abstractmethod
    async def run(self, job: "AcquisitionJob") -> None:  # type: ignore[name-defined]  # noqa: F821
        """Exécute le job. Modifie l'état du job au fur et à mesure."""
        ...

    @abstractmethod
    async def cancel(self, job_id: str) -> None:
        """Annule l'exécution d'un job spécifique."""
        ...
