"""
AcquisitionJob — entité de domaine centrale de la plateforme.

C'est l'objet autour duquel tous les composants collaborent.
Il représente une exécution complète d'acquisition depuis une source.

Contrairement à AcquisitionJobRecord (snapshot immuable dans contracts/),
l'AcquisitionJob est une entité mutable avec un cycle de vie complet.

Les composants ne s'appellent pas directement :
  Channel → Dispatcher → Worker → [AcquisitionJob] → Pipeline → Repository

L'AcquisitionJob transporte le contexte d'exécution et accumule les résultats.
Il est l'unique source de vérité sur l'état d'une acquisition en cours.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from civitas_acquisition.contracts.models.acquisition_job import (
    AcquisitionJobRecord,
    JobStatus,
    JobTrigger,
    new_job_id,
)
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.errors.base import AcquisitionError


class InvalidJobTransitionError(Exception):
    """Levée lors d'une transition de statut invalide."""

    def __init__(self, current: JobStatus, attempted: JobStatus) -> None:
        super().__init__(
            f"Cannot transition from {current.name} to {attempted.name}"
        )


# Transitions de statut autorisées
_ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.PENDING:   {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.RUNNING:   {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED},
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED:    set(),
    JobStatus.CANCELLED: set(),
}


class AcquisitionJob:
    """
    Entité de domaine représentant une exécution complète d'acquisition.

    Cycle de vie :
        PENDING → RUNNING → COMPLETED
                           → FAILED
                 → CANCELLED (depuis PENDING ou RUNNING)

    Usage :
        job = AcquisitionJob.create(
            connector_id="github",
            instance_id="inst-github-1",
            channel_type=ChannelType.POLLING,
            trigger=JobTrigger.SCHEDULED,
            config=config,
        )
        job.start()
        job.increment_acquired(10)
        job.advance_cursor(cursor)
        job.complete()
        record = job.to_record()
    """

    def __init__(
        self,
        job_id: str,
        connector_id: str,
        instance_id: str,
        channel_type: ChannelType,
        trigger: JobTrigger,
        config: ConnectorConfig,
        workspace_id: Optional[str] = None,
    ) -> None:
        self._job_id = job_id
        self._connector_id = connector_id
        self._instance_id = instance_id
        self._channel_type = channel_type
        self._trigger = trigger
        self._config = config
        self._workspace_id = workspace_id

        self._status = JobStatus.PENDING
        self._created_at: datetime = datetime.now(tz=timezone.utc)
        self._started_at: Optional[datetime] = None
        self._completed_at: Optional[datetime] = None

        self._starting_cursor: Optional[Cursor] = None
        self._current_cursor: Optional[Cursor] = None

        self._documents_acquired: int = 0
        self._documents_skipped: int = 0
        self._documents_failed: int = 0

        self._errors: list[AcquisitionError] = []
        self._failure_reason: Optional[str] = None
        self._metadata: dict[str, Any] = {}

    @classmethod
    def create(
        cls,
        connector_id: str,
        instance_id: str,
        channel_type: ChannelType,
        trigger: JobTrigger,
        config: ConnectorConfig,
        starting_cursor: Optional[Cursor] = None,
        workspace_id: Optional[str] = None,
    ) -> AcquisitionJob:
        job = cls(
            job_id=new_job_id(),
            connector_id=connector_id,
            instance_id=instance_id,
            channel_type=channel_type,
            trigger=trigger,
            config=config,
            workspace_id=workspace_id,
        )
        job._starting_cursor = starting_cursor
        job._current_cursor = starting_cursor
        return job

    # ── Transitions d'état ────────────────────────────────────────────────────

    def _transition(self, new_status: JobStatus) -> None:
        allowed = _ALLOWED_TRANSITIONS.get(self._status, set())
        if new_status not in allowed:
            raise InvalidJobTransitionError(self._status, new_status)
        self._status = new_status

    def start(self) -> None:
        self._transition(JobStatus.RUNNING)
        self._started_at = datetime.now(tz=timezone.utc)

    def complete(self) -> None:
        self._transition(JobStatus.COMPLETED)
        self._completed_at = datetime.now(tz=timezone.utc)

    def fail(self, reason: str, error: Optional[AcquisitionError] = None) -> None:
        self._transition(JobStatus.FAILED)
        self._completed_at = datetime.now(tz=timezone.utc)
        self._failure_reason = reason
        if error:
            self._errors.append(error)

    def cancel(self) -> None:
        self._transition(JobStatus.CANCELLED)
        self._completed_at = datetime.now(tz=timezone.utc)

    # ── Accumulation de résultats ─────────────────────────────────────────────

    def increment_acquired(self, count: int = 1) -> None:
        self._documents_acquired += count

    def increment_skipped(self, count: int = 1) -> None:
        self._documents_skipped += count

    def increment_failed(self, count: int = 1, error: Optional[AcquisitionError] = None) -> None:
        self._documents_failed += count
        if error:
            self._errors.append(error)

    def advance_cursor(self, cursor: Cursor) -> None:
        """Avance le curseur. Appelé APRÈS écriture réussie dans le repository."""
        self._current_cursor = cursor

    def set_metadata(self, key: str, value: Any) -> None:
        self._metadata[key] = value

    # ── Accesseurs ────────────────────────────────────────────────────────────

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def connector_id(self) -> str:
        return self._connector_id

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def status(self) -> JobStatus:
        return self._status

    @property
    def config(self) -> ConnectorConfig:
        return self._config

    @property
    def current_cursor(self) -> Optional[Cursor]:
        return self._current_cursor

    @property
    def is_active(self) -> bool:
        return self._status == JobStatus.RUNNING

    @property
    def is_terminal(self) -> bool:
        return self._status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

    @property
    def duration_ms(self) -> Optional[float]:
        if self._started_at is None or self._completed_at is None:
            return None
        return (self._completed_at - self._started_at).total_seconds() * 1000

    @property
    def errors(self) -> list[AcquisitionError]:
        return list(self._errors)

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def to_record(self) -> AcquisitionJobRecord:
        """Produit un snapshot immuable de l'état courant."""
        return AcquisitionJobRecord(
            job_id=self._job_id,
            connector_id=self._connector_id,
            instance_id=self._instance_id,
            channel_type=self._channel_type,
            trigger=self._trigger,
            status=self._status,
            created_at=self._created_at,
            started_at=self._started_at,
            completed_at=self._completed_at,
            starting_cursor_value=(
                self._starting_cursor.value if self._starting_cursor else None
            ),
            final_cursor_value=(
                self._current_cursor.value if self._current_cursor else None
            ),
            documents_acquired=self._documents_acquired,
            documents_skipped=self._documents_skipped,
            documents_failed=self._documents_failed,
            workspace_id=self._workspace_id,
            metadata=dict(self._metadata),
            failure_reason=self._failure_reason,
        )

    def __repr__(self) -> str:
        return (
            f"AcquisitionJob("
            f"id={self._job_id[:8]}..., "
            f"connector={self._connector_id!r}, "
            f"status={self._status.name}, "
            f"acquired={self._documents_acquired})"
        )
