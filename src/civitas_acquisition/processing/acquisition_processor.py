"""
AcquisitionProcessor — orchestrateur central du traitement d'un RawDocument.

Remplace le terme "Pipeline" par "Processor" — plus juste sémantiquement.
Un processor traite UN document. Il n'ordonne pas une séquence globale.

Séquence de traitement pour chaque RawDocument :
  1. Validation  (schema + content)
  2. Déduplication
  3. Écriture dans le Raw Repository
  4. Avancement du curseur dans l'AcquisitionJob
  5. Émission de l'événement RawDocumentCreated

Tout est injectable. Zéro logique hardcodée.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.models.events import (
    RawDocumentCreated,
    AcquisitionFailed,
    DocumentDeduplicated,
)
from civitas_acquisition.contracts.ports.raw_repository_port import RawRepositoryPort
from civitas_acquisition.contracts.ports.event_bus_port import EventBusPort
from civitas_acquisition.contracts.errors.validation_errors import ValidationError
from civitas_acquisition.runtime.resilience.dlq import InMemoryDLQ, DLQEntry, FailureType
from civitas_acquisition.runtime.telemetry.metrics import AcquisitionMetrics
from civitas_acquisition.processing.validators.document_validator import ValidatorPort
from civitas_acquisition.processing.deduplicators.deduplicator import DeduplicatorPort
from civitas_acquisition.domain.acquisition_job import AcquisitionJob

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    status: Literal["done", "skipped", "failed"]
    doc_id: str = ""
    reason: str = ""

    @property
    def success(self) -> bool:
        return self.status in ("done", "skipped")


class AcquisitionProcessor:
    """
    Traite un RawDocument de bout en bout.
    Appelé par le Worker pour chaque document émis par un connecteur.
    """

    def __init__(
        self,
        validator: ValidatorPort,
        deduplicator: DeduplicatorPort,
        repository: RawRepositoryPort,
        event_bus: EventBusPort,
        dlq: InMemoryDLQ,
        metrics: AcquisitionMetrics,
    ) -> None:
        self._validator = validator
        self._deduplicator = deduplicator
        self._repo = repository
        self._bus = event_bus
        self._dlq = dlq
        self._metrics = metrics

    async def process(
        self,
        doc: RawDocument,
        job: AcquisitionJob | None = None,
    ) -> ProcessingResult:
        """
        Traite un document. Met à jour le job si fourni.
        """
        connector_id = doc.source_ref.connector_id
        instance_id = doc.source_ref.instance_id

        # ── 1. Validation ────────────────────────────────────────────────────
        try:
            self._validator.validate(doc)
        except ValidationError as exc:
            logger.warning("Validation failed for %s: %s", doc.id[:12], exc)
            self._metrics.validation_errors.inc(connector_id=connector_id)

            entry = DLQEntry.from_exception(
                raw_doc=doc,
                error=exc,
                failure_type=FailureType.VALIDATION,
                connector_id=connector_id,
                instance_id=instance_id,
            )
            await self._dlq.enqueue(entry)
            self._metrics.dlq_enqueued.inc(connector_id=connector_id)

            await self._bus.emit(AcquisitionFailed(
                connector_id=connector_id,
                instance_id=instance_id,
                uri=doc.source_ref.uri,
                failure_type="validation",
                error_message=str(exc),
                document_id=doc.id,
            ))
            if job:
                job.increment_failed()
            return ProcessingResult(status="failed", doc_id=doc.id, reason=str(exc))

        # ── 2. Déduplication ─────────────────────────────────────────────────
        if await self._deduplicator.is_duplicate(doc):
            logger.debug("Duplicate skipped: %s", doc.id[:12])
            self._metrics.dedup_hits.inc(connector_id=connector_id)
            await self._bus.emit(DocumentDeduplicated(
                document_id=doc.id,
                connector_id=connector_id,
                instance_id=instance_id,
                uri=doc.source_ref.uri,
                dedup_strategy="id_hash",
            ))
            if job:
                job.increment_skipped()
            return ProcessingResult(status="skipped", doc_id=doc.id, reason="duplicate")

        # ── 3. Écriture dans le Repository ───────────────────────────────────
        try:
            await self._repo.write(doc)
            await self._deduplicator.mark_seen(doc)
        except Exception as exc:
            logger.error("Repository write failed for %s: %s", doc.id[:12], exc)
            entry = DLQEntry.from_exception(
                raw_doc=doc,
                error=exc,
                failure_type=FailureType.REPOSITORY,
                connector_id=connector_id,
                instance_id=instance_id,
            )
            await self._dlq.enqueue(entry)
            self._metrics.dlq_enqueued.inc(connector_id=connector_id)
            if job:
                job.increment_failed()
            return ProcessingResult(status="failed", doc_id=doc.id, reason=str(exc))

        # ── 4. Curseur ───────────────────────────────────────────────────────
        # Avancé UNIQUEMENT après écriture réussie — exactly-once guarantee
        if job and doc.cursor:
            job.advance_cursor(doc.cursor)

        # ── 5. Événement ─────────────────────────────────────────────────────
        await self._bus.emit(RawDocumentCreated(
            document_id=doc.id,
            connector_id=connector_id,
            instance_id=instance_id,
            uri=doc.source_ref.uri,
            content_type=doc.content_type,
            size_bytes=doc.size_bytes,
        ))

        self._metrics.documents_acquired.inc(
            connector_id=connector_id,
            instance_id=instance_id,
        )
        if job:
            job.increment_acquired()

        logger.debug("Processed %s (%d bytes)", doc.id[:12], doc.size_bytes)
        return ProcessingResult(status="done", doc_id=doc.id)
