"""Tests unitaires pour RawDocument, SourceReference, et les fonctions utilitaires."""

import pytest
from datetime import timezone

from civitas_acquisition.contracts.models.raw_document import (
    RawDocument,
    SourceReference,
    make_document_id,
    sha256_checksum,
)
from civitas_acquisition.contracts.models.cursor import Cursor


# ── make_document_id ─────────────────────────────────────────────────────────

class TestMakeDocumentId:

    def test_deterministique_meme_entree(self):
        id1 = make_document_id("inst-1", "https://github.com/repo/file.md")
        id2 = make_document_id("inst-1", "https://github.com/repo/file.md")
        assert id1 == id2

    def test_instances_differentes_ids_differents(self):
        id1 = make_document_id("inst-1", "https://example.com/doc.pdf")
        id2 = make_document_id("inst-2", "https://example.com/doc.pdf")
        assert id1 != id2

    def test_uris_differents_ids_differents(self):
        id1 = make_document_id("inst-1", "https://example.com/doc1.pdf")
        id2 = make_document_id("inst-1", "https://example.com/doc2.pdf")
        assert id1 != id2

    def test_format_hex_64_chars(self):
        doc_id = make_document_id("inst-1", "https://example.com")
        assert len(doc_id) == 64
        assert all(c in "0123456789abcdef" for c in doc_id)

    def test_instance_vide_uri_vide(self):
        """Les cas limites doivent quand même produire un ID valide."""
        doc_id = make_document_id("", "")
        assert len(doc_id) == 64


# ── sha256_checksum ───────────────────────────────────────────────────────────

class TestSha256Checksum:

    def test_coherent_meme_contenu(self):
        content = b"Hello, CIVITAS!"
        assert sha256_checksum(content) == sha256_checksum(content)

    def test_contenu_different_checksum_different(self):
        assert sha256_checksum(b"content-a") != sha256_checksum(b"content-b")

    def test_contenu_vide(self):
        checksum = sha256_checksum(b"")
        assert len(checksum) == 64

    def test_format_hex(self):
        checksum = sha256_checksum(b"test data")
        assert all(c in "0123456789abcdef" for c in checksum)


# ── RawDocument.create ────────────────────────────────────────────────────────

class TestRawDocumentCreate:

    def test_id_calcule_automatiquement(self):
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="github",
            uri="https://github.com/org/repo/file.md",
            content=b"# Title",
            content_type="text/markdown",
        )
        expected = make_document_id("inst-1", "https://github.com/org/repo/file.md")
        assert doc.id == expected

    def test_checksum_calcule_automatiquement(self):
        content = b"Important content here"
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="notion",
            uri="https://notion.so/page/abc123",
            content=content,
            content_type="text/html",
        )
        assert doc.checksum == sha256_checksum(content)

    def test_size_bytes_calcule_automatiquement(self):
        content = b"x" * 1024
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="s3",
            uri="s3://bucket/file.txt",
            content=content,
            content_type="text/plain",
        )
        assert doc.size_bytes == 1024

    def test_acquired_at_utc(self):
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="slack",
            uri="slack://channel/msg/123",
            content=b"message content",
            content_type="text/plain",
        )
        assert doc.acquired_at.tzinfo == timezone.utc

    def test_idempotent_memes_kwargs(self):
        kwargs = dict(
            instance_id="inst-1",
            connector_id="s3",
            uri="s3://my-bucket/my-file.pdf",
            content=b"PDF bytes here",
            content_type="application/pdf",
        )
        doc1 = RawDocument.create(**kwargs)
        doc2 = RawDocument.create(**kwargs)
        assert doc1.id == doc2.id
        assert doc1.checksum == doc2.checksum

    def test_avec_cursor(self):
        cursor = Cursor(
            value="2024-01-15T10:00:00Z",
            source_type="timestamp",
            connector_id="github",
            instance_id="inst-1",
        )
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="github",
            uri="https://github.com/file.txt",
            content=b"content",
            content_type="text/plain",
            cursor=cursor,
        )
        assert doc.cursor == cursor

    def test_avec_tags(self):
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="github",
            uri="https://github.com/file.txt",
            content=b"content",
            content_type="text/plain",
            tags=("python", "documentation"),
        )
        assert "python" in doc.tags
        assert "documentation" in doc.tags

    def test_sans_version(self):
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="rss",
            uri="https://blog.example.com/feed/1",
            content=b"<rss>...</rss>",
            content_type="application/rss+xml",
        )
        assert doc.source_ref.version is None

    def test_avec_version(self):
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="github",
            uri="https://github.com/repo/file.md",
            content=b"content",
            content_type="text/markdown",
            version="abc123sha",
        )
        assert doc.source_ref.version == "abc123sha"

    def test_source_metadata_vide_par_defaut(self):
        doc = RawDocument.create(
            instance_id="inst-1",
            connector_id="notion",
            uri="https://notion.so/page/xyz",
            content=b"content",
            content_type="text/html",
        )
        assert doc.source_metadata == {}


# ── RawDocument propriétés ────────────────────────────────────────────────────

class TestRawDocumentProperties:

    def _make_doc(self, content_type: str = "text/plain") -> RawDocument:
        return RawDocument.create(
            instance_id="inst-1",
            connector_id="test",
            uri="https://example.com/doc",
            content=b"sample content",
            content_type=content_type,
        )

    def test_est_immutable(self):
        doc = self._make_doc()
        with pytest.raises((AttributeError, TypeError)):
            doc.content_type = "application/json"  # type: ignore[misc]

    def test_is_text_pour_text_types(self):
        assert self._make_doc("text/plain").is_text() is True
        assert self._make_doc("text/html").is_text() is True
        assert self._make_doc("text/markdown").is_text() is True

    def test_is_binary_pour_binaires(self):
        assert self._make_doc("application/pdf").is_binary() is True
        assert self._make_doc("image/png").is_binary() is True

    def test_verify_integrity_ok(self):
        doc = self._make_doc()
        assert doc.verify_integrity() is True

    def test_repr_tronque_id(self):
        doc = self._make_doc()
        r = repr(doc)
        assert "..." in r
        assert "test" in r

    def test_source_ref_connector_id(self):
        doc = self._make_doc()
        assert doc.source_ref.connector_id == "test"
        assert doc.source_ref.instance_id == "inst-1"


# ── Cursor ────────────────────────────────────────────────────────────────────

class TestCursor:

    def test_est_immutable(self):
        cursor = Cursor(
            value="2024-01-01", source_type="timestamp",
            connector_id="github", instance_id="inst-1"
        )
        with pytest.raises((AttributeError, TypeError)):
            cursor.value = "modified"  # type: ignore[misc]

    def test_serialisation_aller_retour(self):
        original = Cursor(
            value="offset-12345",
            source_type="offset",
            connector_id="kafka",
            instance_id="inst-kafka-1",
        )
        restored = Cursor.from_dict(original.to_dict())
        assert restored == original

    def test_str_representation(self):
        cursor = Cursor(
            value="page-3", source_type="page",
            connector_id="notion", instance_id="inst-1"
        )
        s = str(cursor)
        assert "page" in s
        assert "page-3" in s
