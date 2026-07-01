"""
Événements du domaine Acquisition.

Tous les événements sont immuables et self-contained.
L'EventBus in-process les dispatche aux handlers abonnés.
Aucune sérialisation réseau ici — ce sont des objets Python purs.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4


def _new_event_id() -> str:
    return str(uuid4())


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class AcquisitionEvent:
    """Événement racine. Tous les événements héritent de cette classe."""
    event_id: str = field(default_factory=_new_event_id)
    occurred_at: datetime = field(default_factory=_now_utc)


@dataclass(frozen=True)
class RawDocumentCreated(AcquisitionEvent):
    """
    Émis quand un RawDocument est écrit avec succès dans le Raw Repository.
    C'est le signal pour la Transformation Platform de démarrer.
    """
    document_id: str = ""
    connector_id: str = ""
    instance_id: str = ""
    uri: str = ""
    content_type: str = ""
    size_bytes: int = 0
    workspace_id: Optional[str] = None


@dataclass(frozen=True)
class AcquisitionFailed(AcquisitionEvent):
    """
    Émis quand un document ne peut pas être acquis ou traité.
    Le document est envoyé en DLQ.
    """
    connector_id: str = ""
    instance_id: str = ""
    uri: str = ""
    failure_type: str = ""       # "validation", "network", "auth", "unexpected"
    error_message: str = ""
    document_id: Optional[str] = None


@dataclass(frozen=True)
class DocumentDeduplicated(AcquisitionEvent):
    """Émis quand un document est ignoré car déjà connu du système."""
    document_id: str = ""
    connector_id: str = ""
    instance_id: str = ""
    uri: str = ""
    dedup_strategy: str = ""     # "exact_hash", "near_duplicate"


@dataclass(frozen=True)
class CursorAdvanced(AcquisitionEvent):
    """
    Émis quand le curseur d'une instance est avancé après écriture réussie.
    Utile pour le monitoring de la progression de l'acquisition.
    """
    connector_id: str = ""
    instance_id: str = ""
    cursor_value: str = ""
    cursor_type: str = ""


@dataclass(frozen=True)
class ConnectorHealthChanged(AcquisitionEvent):
    """
    Émis lors d'un changement d'état de santé d'une instance connecteur.
    Permet au monitoring de réagir en temps réel.
    """
    connector_id: str = ""
    instance_id: str = ""
    healthy: bool = False
    latency_ms: Optional[float] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class CircuitBreakerStateChanged(AcquisitionEvent):
    """Émis lors d'un changement d'état du circuit breaker."""
    resource_id: str = ""
    previous_state: str = ""   # "closed", "open", "half_open"
    new_state: str = ""
    failure_count: int = 0


@dataclass(frozen=True)
class DLQDocumentEnqueued(AcquisitionEvent):
    """Émis quand un document échoué est placé en Dead Letter Queue."""
    document_id: str = ""
    connector_id: str = ""
    instance_id: str = ""
    failure_type: str = ""
    error_message: str = ""
    attempt_count: int = 0
