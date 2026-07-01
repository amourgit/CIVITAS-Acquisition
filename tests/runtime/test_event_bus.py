"""Tests unitaires pour l'InProcessEventBus."""

import pytest
from civitas_acquisition.runtime.eventbus.in_process import InProcessEventBus
from civitas_acquisition.contracts.models.events import (
    RawDocumentCreated,
    AcquisitionFailed,
    AcquisitionEvent,
)


class TestInProcessEventBus:

    @pytest.fixture
    def bus(self):
        return InProcessEventBus()

    async def test_emit_appelle_handler(self, bus):
        received = []
        async def handler(event): received.append(event)
        bus.subscribe(RawDocumentCreated, handler)
        event = RawDocumentCreated(document_id="doc-1", connector_id="rss")
        await bus.emit(event)
        assert len(received) == 1
        assert received[0].document_id == "doc-1"

    async def test_handler_non_appele_pour_autre_type(self, bus):
        received = []
        async def handler(event): received.append(event)
        bus.subscribe(RawDocumentCreated, handler)
        await bus.emit(AcquisitionFailed(connector_id="rss"))
        assert len(received) == 0

    async def test_plusieurs_handlers_meme_type(self, bus):
        calls = []
        async def h1(e): calls.append("h1")
        async def h2(e): calls.append("h2")
        bus.subscribe(RawDocumentCreated, h1)
        bus.subscribe(RawDocumentCreated, h2)
        await bus.emit(RawDocumentCreated())
        assert "h1" in calls and "h2" in calls

    async def test_unsubscribe_stoppe_reception(self, bus):
        received = []
        async def handler(event): received.append(event)
        sub = bus.subscribe(RawDocumentCreated, handler)
        bus.unsubscribe(sub)
        await bus.emit(RawDocumentCreated())
        assert len(received) == 0

    async def test_erreur_dans_handler_isolee(self, bus):
        async def bad_handler(e): raise RuntimeError("boom")
        async def good_handler(e): good_calls.append(e)
        good_calls = []
        bus.subscribe(RawDocumentCreated, bad_handler)
        bus.subscribe(RawDocumentCreated, good_handler)
        await bus.emit(RawDocumentCreated())   # ne doit pas lever
        assert len(good_calls) == 1

    async def test_heritage_evenement(self, bus):
        """Les handlers abonnés à AcquisitionEvent reçoivent tous les sous-types."""
        received = []
        async def handler(e): received.append(e)
        bus.subscribe(AcquisitionEvent, handler)
        await bus.emit(RawDocumentCreated())
        assert len(received) == 1

    def test_subscriber_count(self, bus):
        async def h(e): pass
        assert bus.subscriber_count(RawDocumentCreated) == 0
        bus.subscribe(RawDocumentCreated, h)
        assert bus.subscriber_count(RawDocumentCreated) == 1

    def test_clear(self, bus):
        async def h(e): pass
        bus.subscribe(RawDocumentCreated, h)
        bus.clear()
        assert bus.subscriber_count(RawDocumentCreated) == 0
