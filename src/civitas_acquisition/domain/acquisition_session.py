"""
AcquisitionSession — groupe de jobs d'une même instance connecteur.

Une session représente le cycle de vie long d'une instance connecteur active :
depuis son démarrage jusqu'à son arrêt, elle accumule les jobs exécutés.

Utile pour :
  - Tracking du curseur cumulatif (progression globale dans la source)
  - Agrégation des métriques (documents totaux par session)
  - Reprise après redémarrage (état persisté de la session)
  - Monitoring (heartbeat, santé de l'instance)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.acquisition_job import (
    AcquisitionJobRecord, JobStatus,
)


class AcquisitionSession:
    """
    Session d'acquisition pour une instance connecteur.
    Agrège les jobs successifs et maintient la progression globale.
    """

    def __init__(
        self,
        session_id: str,
        connector_id: str,
        instance_id: str,
        workspace_id: Optional[str] = None,
    ) -> None:
        self._session_id = session_id
        self._connector_id = connector_id
        self._instance_id = instance_id
        self._workspace_id = workspace_id

        self._started_at: datetime = datetime.now(tz=timezone.utc)
        self._last_heartbeat: datetime = self._started_at
        self._stopped_at: Optional[datetime] = None
        self._active = True

        self._jobs: list[AcquisitionJobRecord] = []
        self._last_cursor: Optional[Cursor] = None

    @classmethod
    def create(
        cls,
        connector_id: str,
        instance_id: str,
        workspace_id: Optional[str] = None,
    ) -> AcquisitionSession:
        return cls(
            session_id=str(uuid4()),
            connector_id=connector_id,
            instance_id=instance_id,
            workspace_id=workspace_id,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def heartbeat(self) -> None:
        """Enregistre un heartbeat. Appelé périodiquement par le channel actif."""
        self._last_heartbeat = datetime.now(tz=timezone.utc)

    def stop(self) -> None:
        """Marque la session comme terminée."""
        self._active = False
        self._stopped_at = datetime.now(tz=timezone.utc)

    # ── Accumulation ──────────────────────────────────────────────────────────

    def record_job(self, job_record: AcquisitionJobRecord) -> None:
        """Enregistre le record d'un job terminé dans la session."""
        self._jobs.append(job_record)
        if job_record.final_cursor_value and job_record.status == JobStatus.COMPLETED:
            # On avance le curseur de session uniquement sur succès
            if self._last_cursor is None or job_record.final_cursor_value > (
                self._last_cursor.value
            ):
                self._last_cursor = Cursor(
                    value=job_record.final_cursor_value,
                    source_type="token",   # sera précisé par le connecteur
                    connector_id=self._connector_id,
                    instance_id=self._instance_id,
                )

    # ── Métriques agrégées ────────────────────────────────────────────────────

    @property
    def total_documents_acquired(self) -> int:
        return sum(j.documents_acquired for j in self._jobs)

    @property
    def total_documents_failed(self) -> int:
        return sum(j.documents_failed for j in self._jobs)

    @property
    def total_jobs(self) -> int:
        return len(self._jobs)

    @property
    def successful_jobs(self) -> int:
        return sum(1 for j in self._jobs if j.status == JobStatus.COMPLETED)

    @property
    def failed_jobs(self) -> int:
        return sum(1 for j in self._jobs if j.status == JobStatus.FAILED)

    # ── Accesseurs ────────────────────────────────────────────────────────────

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def connector_id(self) -> str:
        return self._connector_id

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def last_cursor(self) -> Optional[Cursor]:
        return self._last_cursor

    @property
    def last_heartbeat(self) -> datetime:
        return self._last_heartbeat

    @property
    def uptime_s(self) -> float:
        end = self._stopped_at or datetime.now(tz=timezone.utc)
        return (end - self._started_at).total_seconds()

    def __repr__(self) -> str:
        status = "active" if self._active else "stopped"
        return (
            f"AcquisitionSession("
            f"id={self._session_id[:8]}..., "
            f"connector={self._connector_id!r}, "
            f"jobs={self.total_jobs}, "
            f"status={status})"
        )
