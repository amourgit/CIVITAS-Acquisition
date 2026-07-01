"""
DeadLetterQueue — file des documents ayant échoué leur traitement.

Chaque document placé en DLQ contient :
  - Le RawDocument original (pour replay)
  - L'exception capturée (type + message + stack trace)
  - Le contexte complet (connector, instance, tentatives)
  - Le timestamp d'échec

Opérations :
  - enqueue    : placer un document en échec
  - list       : lister les entrées avec filtres
  - replay     : réinjecter des entrées dans le pipeline
  - dismiss    : supprimer définitivement une entrée
  - stats      : métriques agrégées

Implémentation par défaut : in-memory (pour les tests et le dev).
En production : PostgresDLQ ou RedisDLQ via le pattern Port/Adapter.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Optional
from uuid import uuid4

from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.base import AcquisitionError


class FailureType(Enum):
    VALIDATION    = auto()
    NETWORK       = auto()
    AUTH          = auto()
    RATE_LIMIT    = auto()
    DEDUPLICATION = auto()
    REPOSITORY    = auto()
    UNEXPECTED    = auto()


@dataclass
class DLQEntry:
    """Une entrée dans la Dead Letter Queue."""
    entry_id: str = field(default_factory=lambda: str(uuid4()))
    raw_doc: Optional[RawDocument] = None
    failure_type: FailureType = FailureType.UNEXPECTED
    error_type: str = ""
    error_message: str = ""
    stack_trace: str = ""
    connector_id: str = ""
    instance_id: str = ""
    attempt_count: int = 1
    failed_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    dismissed: bool = False
    dismiss_reason: Optional[str] = None

    @classmethod
    def from_exception(
        cls,
        raw_doc: Optional[RawDocument],
        error: Exception,
        failure_type: FailureType,
        connector_id: str,
        instance_id: str,
        attempt_count: int = 1,
    ) -> DLQEntry:
        return cls(
            raw_doc=raw_doc,
            failure_type=failure_type,
            error_type=type(error).__name__,
            error_message=str(error),
            stack_trace=traceback.format_exc(),
            connector_id=connector_id,
            instance_id=instance_id,
            attempt_count=attempt_count,
        )

    def __repr__(self) -> str:
        doc_id = self.raw_doc.id[:12] if self.raw_doc else "N/A"
        return (
            f"DLQEntry("
            f"id={self.entry_id[:8]}..., "
            f"doc={doc_id}..., "
            f"type={self.failure_type.name}, "
            f"error={self.error_type})"
        )


@dataclass
class DLQFilters:
    connector_id: Optional[str] = None
    instance_id: Optional[str] = None
    failure_type: Optional[FailureType] = None
    since: Optional[datetime] = None
    dismissed: bool = False
    limit: int = 100


@dataclass
class DLQStats:
    total: int = 0
    by_failure_type: dict[str, int] = field(default_factory=dict)
    by_connector: dict[str, int] = field(default_factory=dict)
    oldest_entry_at: Optional[datetime] = None


class InMemoryDLQ:
    """
    Implémentation in-memory de la Dead Letter Queue.
    Pour dev et tests. En production, utiliser PostgresDLQ.
    """

    def __init__(self) -> None:
        self._entries: dict[str, DLQEntry] = {}

    async def enqueue(self, entry: DLQEntry) -> None:
        self._entries[entry.entry_id] = entry

    async def list(self, filters: DLQFilters) -> list[DLQEntry]:
        result = [e for e in self._entries.values() if not e.dismissed or filters.dismissed]
        if filters.connector_id:
            result = [e for e in result if e.connector_id == filters.connector_id]
        if filters.failure_type:
            result = [e for e in result if e.failure_type == filters.failure_type]
        if filters.since:
            result = [e for e in result if e.failed_at >= filters.since]
        result.sort(key=lambda e: e.failed_at)
        return result[: filters.limit]

    async def get(self, entry_id: str) -> Optional[DLQEntry]:
        return self._entries.get(entry_id)

    async def dismiss(self, entry_id: str, reason: str) -> None:
        if entry := self._entries.get(entry_id):
            entry.dismissed = True
            entry.dismiss_reason = reason

    async def stats(self) -> DLQStats:
        active = [e for e in self._entries.values() if not e.dismissed]
        by_type: dict[str, int] = {}
        by_conn: dict[str, int] = {}
        oldest: Optional[datetime] = None
        for e in active:
            by_type[e.failure_type.name] = by_type.get(e.failure_type.name, 0) + 1
            by_conn[e.connector_id] = by_conn.get(e.connector_id, 0) + 1
            if oldest is None or e.failed_at < oldest:
                oldest = e.failed_at
        return DLQStats(
            total=len(active),
            by_failure_type=by_type,
            by_connector=by_conn,
            oldest_entry_at=oldest,
        )

    def size(self) -> int:
        return sum(1 for e in self._entries.values() if not e.dismissed)
