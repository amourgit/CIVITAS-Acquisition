"""
Test d'intégration end-to-end du AcquisitionProcessor.

Vérifie la collaboration entre :
  Validator → Deduplicator → LocalRawRepository → EventBus → DLQ
"""
import pytest
import tempfile
import os

from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.models.events import RawDocumentCreated, DocumentDeduplicated, AcquisitionFailed
from civitas_acquisition.processing.validators.document_validator import default_validator
from civitas_acquisition.processing.deduplicators.deduplicator import InMemoryDeduplicator
from civitas_acquisition.processing.acquisition_processor import AcquisitionProcessor
from civitas_acquisition.storage.local import LocalRawRepository
from civitas_acquisition.runtime.eventbus.in_process import InProcessEventBus
from civitas_acquisition.runtime.resilience.dlq import InMemoryDLQ
from civitas_acquisition.runtime.telemetry.metrics import AcquisitionMetrics


@pytest.fixture
def tmp_repo(tmp_path):
    return LocalRawRepository(base_dir=str(tmp_path))


@pytest.fixture
def bus():
    return InProcessEventBus()


@pytest.fixture
def processor(tmp_repo, bus):
    return AcquisitionProcessor(
        validator=default_validator(),
        deduplicator=InMemoryDeduplicator(),
        repository=tmp_repo,
        event_bus=bus,
        dlq=InMemoryDLQ(),
        metrics=AcquisitionMetrics(),
    )


def make_doc(uri: str = "https://blog.example.com/post/1") -> RawDocument:
    return RawDocument.create(
        instance_id="inst-rss-1",
        connector_id="rss",
        uri=uri,
        content=b"This is the article content about AI.",
        content_type="text/plain",
    )


class TestAcquisitionProcessorE2E:

    async def test_traitement_complet_nouveau_doc(self, processor, tmp_repo, bus):
        received_events = []
        async def capture(e): received_events.append(e)
        bus.subscribe(RawDocumentCreated, capture)

        doc = make_doc()
        result = await processor.process(doc)

        assert result.status == "done"
        assert result.doc_id == doc.id
        assert await tmp_repo.exists(doc.id)
        assert len(received_events) == 1
        assert received_events[0].document_id == doc.id

    async def test_document_duplique_skippe(self, processor, bus):
        dedup_events = []
        async def capture(e): dedup_events.append(e)
        bus.subscribe(DocumentDeduplicated, capture)

        doc = make_doc()
        r1 = await processor.process(doc)
        r2 = await processor.process(doc)   # même doc

        assert r1.status == "done"
        assert r2.status == "skipped"
        assert r2.reason == "duplicate"
        assert len(dedup_events) == 1

    async def test_doc_invalide_va_en_dlq(self, processor, bus):
        import dataclasses
        failed_events = []
        async def capture(e): failed_events.append(e)
        bus.subscribe(AcquisitionFailed, capture)

        doc = make_doc()
        bad_doc = dataclasses.replace(doc, content=b"", size_bytes=0)
        result = await processor.process(bad_doc)

        assert result.status == "failed"
        assert len(failed_events) == 1

    async def test_plusieurs_docs_differents(self, processor, tmp_repo):
        uris = [f"https://blog.example.com/post/{i}" for i in range(5)]
        docs = [make_doc(uri) for uri in uris]

        for doc in docs:
            result = await processor.process(doc)
            assert result.status == "done"

        assert await tmp_repo.count(
            __import__("civitas_acquisition.contracts.ports.raw_repository_port",
                       fromlist=["RepositoryFilters"]).RepositoryFilters()
        ) == 5

    async def test_metrics_incrementees(self, tmp_repo, bus):
        metrics = AcquisitionMetrics()
        proc = AcquisitionProcessor(
            validator=default_validator(),
            deduplicator=InMemoryDeduplicator(),
            repository=tmp_repo,
            event_bus=bus,
            dlq=InMemoryDLQ(),
            metrics=metrics,
        )
        doc = make_doc("https://example.com/unique-metrics-test")
        await proc.process(doc)
        assert metrics.documents_acquired.total() == 1
