"""Tests unitaires pour les deduplicators."""

import pytest
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.processing.deduplicators.deduplicator import (
    InMemoryDeduplicator,
)


def make_doc(uri: str = "https://example.com/article") -> RawDocument:
    return RawDocument.create(
        instance_id="inst-1", connector_id="rss",
        uri=uri, content=b"content here", content_type="text/plain",
    )


class TestInMemoryDeduplicator:

    @pytest.fixture
    def dedup(self):
        return InMemoryDeduplicator(max_size=100)

    async def test_nouveau_doc_non_duplique(self, dedup):
        doc = make_doc()
        assert await dedup.is_duplicate(doc) is False

    async def test_apres_mark_seen_est_duplique(self, dedup):
        doc = make_doc()
        await dedup.mark_seen(doc)
        assert await dedup.is_duplicate(doc) is True

    async def test_docs_differents_non_dupliques(self, dedup):
        doc1 = make_doc("https://example.com/article-1")
        doc2 = make_doc("https://example.com/article-2")
        await dedup.mark_seen(doc1)
        assert await dedup.is_duplicate(doc2) is False

    async def test_meme_uri_meme_instance_duplique(self, dedup):
        doc = make_doc("https://example.com/same")
        doc2 = make_doc("https://example.com/same")  # même ID car même instance+uri
        await dedup.mark_seen(doc)
        assert await dedup.is_duplicate(doc2) is True

    async def test_lru_eviction(self):
        dedup = InMemoryDeduplicator(max_size=3)
        docs = [make_doc(f"https://example.com/article-{i}") for i in range(5)]

        for d in docs[:3]:
            await dedup.mark_seen(d)

        # Ajouter un 4ème doc → evict le 1er (LRU)
        await dedup.mark_seen(docs[3])
        assert await dedup.is_duplicate(docs[0]) is False   # evicted
        assert await dedup.is_duplicate(docs[3]) is True    # récent
