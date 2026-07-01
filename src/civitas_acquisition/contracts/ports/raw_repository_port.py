"""
RawRepositoryPort — interface abstraite du Raw Document Repository.

C'est LA frontière de sortie de la plateforme d'Acquisition.
C'est aussi LA frontière d'entrée de la Transformation Platform.

Le repository est le seul point de communication entre les deux plateformes.
Il gère le cycle de vie (status) de chaque document.

Il n'est PAS un simple S3 ou base de données.
Il est un contrat vivant avec :
  - un schéma enforced
  - une couche metadata
  - un state machine par document
  - une couche événement (pour notifier les plateformes downstream)
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import AsyncIterator, Optional

from ..models.raw_document import RawDocument


class DocumentStatus(Enum):
    """
    State machine d'un document dans le Raw Repository.

    PENDING     → écrit par Acquisition, non encore réclamé
    PROCESSING  → réclamé par Transformation (claim-check pattern)
    DONE        → traité avec succès
    FAILED      → le traitement a échoué — voir processing_error
    SKIPPED     → filtré ou dédupliqué intentionnellement
    """
    PENDING    = auto()
    PROCESSING = auto()
    DONE       = auto()
    FAILED     = auto()
    SKIPPED    = auto()


@dataclass
class RepositoryEntry:
    """Un document dans le repository, enrichi de son état de traitement."""
    document: RawDocument
    status: DocumentStatus
    created_at: datetime
    updated_at: datetime
    processed_by: Optional[str] = None       # Nom de la plateforme qui l'a traité
    processing_error: Optional[str] = None
    attempts: int = 0                         # Nombre de tentatives de traitement


@dataclass
class RepositoryFilters:
    """Filtres pour les requêtes sur le repository."""
    status: Optional[DocumentStatus] = None
    connector_id: Optional[str] = None
    instance_id: Optional[str] = None
    workspace_id: Optional[str] = None
    content_type: Optional[str] = None
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    limit: int = 100
    offset: int = 0


class RawRepositoryPort(ABC):
    """
    Interface abstraite du Raw Document Repository.

    Règles fondamentales :
    1. write() est idempotent — écrire deux fois le même document_id
       ne crée pas de doublon (upsert sémantique).
    2. Le status est avancé UNIQUEMENT par update_status().
    3. exists() est l'opération de déduplication principale — O(1).
    4. query() est un async generator pour les grands volumes.
    """

    @abstractmethod
    async def write(self, document: RawDocument) -> None:
        """
        Écrit un document avec status=PENDING.
        Idempotent : si l'ID existe déjà, ne fait rien (pas de doublon).
        """
        ...

    @abstractmethod
    async def read(self, document_id: str) -> Optional[RepositoryEntry]:
        """Lit un document par ID. Retourne None si non trouvé."""
        ...

    @abstractmethod
    def query(self, filters: RepositoryFilters) -> AsyncIterator[RepositoryEntry]:
        """Stream de documents correspondant aux filtres. Async generator."""
        ...

    @abstractmethod
    async def update_status(
        self,
        document_id: str,
        status: DocumentStatus,
        error: Optional[str] = None,
        processed_by: Optional[str] = None,
    ) -> None:
        """Met à jour le statut de traitement d'un document."""
        ...

    @abstractmethod
    async def exists(self, document_id: str) -> bool:
        """
        Vérifie si un document existe déjà.
        Opération de déduplication — doit être O(1).
        """
        ...

    @abstractmethod
    async def count(self, filters: RepositoryFilters) -> int:
        """Compte les documents correspondant aux filtres."""
        ...
