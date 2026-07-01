"""
LocalRawRepository — implémentation filesystem du RawRepositoryPort.

Stocke les documents dans un répertoire local structuré :
  {base_dir}/
    {connector_id}/
      {instance_id}/
        {document_id}.json   ← métadonnées + status
        {document_id}.bin    ← contenu brut

Utilisé pour :
  - Développement local (sans infrastructure)
  - Tests d'intégration
  - Premiers déploiements à petit volume

En production : S3RawRepository, MinIORawRepository, ou PostgresRawRepository.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Optional

from civitas_acquisition.contracts.ports.raw_repository_port import (
    RawRepositoryPort,
    DocumentStatus,
    RepositoryEntry,
    RepositoryFilters,
)
from civitas_acquisition.contracts.models.raw_document import RawDocument, SourceReference
from civitas_acquisition.contracts.models.cursor import Cursor

logger = logging.getLogger(__name__)


class LocalRawRepository(RawRepositoryPort):
    """
    Repository filesystem local. Simple, observable, sans dépendances.
    Idéal pour le développement et les tests unitaires end-to-end.
    """

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)
        # Index en mémoire pour les requêtes rapides (doc_id → status)
        self._index: dict[str, dict] = {}
        self._load_index()

    def _doc_dir(self, connector_id: str, instance_id: str) -> Path:
        return self._base / connector_id / instance_id

    def _meta_path(self, connector_id: str, instance_id: str, doc_id: str) -> Path:
        return self._doc_dir(connector_id, instance_id) / f"{doc_id}.json"

    def _content_path(self, connector_id: str, instance_id: str, doc_id: str) -> Path:
        return self._doc_dir(connector_id, instance_id) / f"{doc_id}.bin"

    def _load_index(self) -> None:
        """Reconstruit l'index en mémoire depuis le filesystem au démarrage."""
        for meta_path in self._base.rglob("*.json"):
            try:
                with open(meta_path) as f:
                    meta = json.load(f)
                self._index[meta["id"]] = meta
            except Exception:
                pass

    async def write(self, document: RawDocument) -> None:
        """Idempotent : si l'ID existe déjà, ne fait rien."""
        if document.id in self._index:
            logger.debug("Document %s already exists — skipping", document.id[:12])
            return

        ref = document.source_ref
        doc_dir = self._doc_dir(ref.connector_id, ref.instance_id)
        doc_dir.mkdir(parents=True, exist_ok=True)

        now = datetime.now(tz=timezone.utc).isoformat()
        meta = {
            "id": document.id,
            "connector_id": ref.connector_id,
            "instance_id": ref.instance_id,
            "uri": ref.uri,
            "version": ref.version,
            "content_type": document.content_type,
            "size_bytes": document.size_bytes,
            "checksum": document.checksum,
            "acquired_at": document.acquired_at.isoformat(),
            "source_metadata": document.source_metadata,
            "tags": list(document.tags),
            "status": DocumentStatus.PENDING.name,
            "created_at": now,
            "updated_at": now,
            "attempts": 0,
            "processing_error": None,
        }

        # Écriture atomique : contenu d'abord, métadonnées ensuite
        content_path = self._content_path(ref.connector_id, ref.instance_id, document.id)
        content_path.write_bytes(document.content)

        meta_path = self._meta_path(ref.connector_id, ref.instance_id, document.id)
        meta_path.write_text(json.dumps(meta, indent=2))

        self._index[document.id] = meta
        logger.debug("Written document %s to %s", document.id[:12], doc_dir)

    async def read(self, document_id: str) -> Optional[RepositoryEntry]:
        meta = self._index.get(document_id)
        if meta is None:
            return None
        return self._meta_to_entry(meta)

    async def exists(self, document_id: str) -> bool:
        return document_id in self._index

    async def update_status(
        self,
        document_id: str,
        status: DocumentStatus,
        error: Optional[str] = None,
        processed_by: Optional[str] = None,
    ) -> None:
        meta = self._index.get(document_id)
        if meta is None:
            logger.warning("update_status: document %s not found", document_id[:12])
            return

        meta["status"] = status.name
        meta["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        if error:
            meta["processing_error"] = error
        if processed_by:
            meta["processed_by"] = processed_by
        if status == DocumentStatus.PROCESSING:
            meta["attempts"] = meta.get("attempts", 0) + 1

        # Persistance
        meta_path = self._meta_path(
            meta["connector_id"], meta["instance_id"], document_id
        )
        meta_path.write_text(json.dumps(meta, indent=2))

    async def count(self, filters: RepositoryFilters) -> int:
        return sum(
            1 for meta in self._index.values()
            if self._matches(meta, filters)
        )

    async def query(self, filters: RepositoryFilters) -> AsyncIterator[RepositoryEntry]:
        count = 0
        for meta in self._index.values():
            if count >= filters.limit:
                break
            if self._matches(meta, filters):
                yield self._meta_to_entry(meta)
                count += 1

    def _matches(self, meta: dict, filters: RepositoryFilters) -> bool:
        if filters.status and meta.get("status") != filters.status.name:
            return False
        if filters.connector_id and meta.get("connector_id") != filters.connector_id:
            return False
        if filters.instance_id and meta.get("instance_id") != filters.instance_id:
            return False
        return True

    def _meta_to_entry(self, meta: dict) -> RepositoryEntry:
        doc = RawDocument(
            id=meta["id"],
            source_ref=SourceReference(
                connector_id=meta["connector_id"],
                instance_id=meta["instance_id"],
                uri=meta["uri"],
                version=meta.get("version"),
            ),
            content=b"",   # Contenu non chargé par défaut (lazy)
            content_type=meta["content_type"],
            size_bytes=meta["size_bytes"],
            checksum=meta["checksum"],
            acquired_at=datetime.fromisoformat(meta["acquired_at"]),
            source_metadata=meta.get("source_metadata", {}),
            tags=tuple(meta.get("tags", [])),
        )
        return RepositoryEntry(
            document=doc,
            status=DocumentStatus[meta["status"]],
            created_at=datetime.fromisoformat(meta["created_at"]),
            updated_at=datetime.fromisoformat(meta["updated_at"]),
            processing_error=meta.get("processing_error"),
            attempts=meta.get("attempts", 0),
        )
