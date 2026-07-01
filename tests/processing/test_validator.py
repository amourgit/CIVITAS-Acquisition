"""Tests unitaires pour les validators du processing."""

import pytest
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.validation_errors import (
    EmptyContentError,
    MissingRequiredFieldError,
    SizeLimitError,
    ChecksumMismatchError,
    SchemaValidationError,
)
from civitas_acquisition.processing.validators.document_validator import (
    SchemaValidator,
    ContentValidator,
    CompositeValidator,
    default_validator,
)


def make_valid_doc(**overrides) -> RawDocument:
    doc = RawDocument.create(
        instance_id="inst-1",
        connector_id="rss",
        uri="https://blog.example.com/feed/1",
        content=b"Article content here",
        content_type="text/plain",
    )
    if overrides:
        # RawDocument est frozen, on utilise replace via dataclasses
        import dataclasses
        return dataclasses.replace(doc, **overrides)
    return doc


class TestSchemaValidator:
    def setup_method(self):
        self.v = SchemaValidator()

    def test_doc_valide_passe(self):
        self.v.validate(make_valid_doc())   # pas d'exception

    def test_id_vide_leve_missing_field(self):
        doc = make_valid_doc(id="")
        with pytest.raises(MissingRequiredFieldError):
            self.v.validate(doc)

    def test_id_trop_court_leve_schema_error(self):
        doc = make_valid_doc(id="abc123")
        with pytest.raises(SchemaValidationError):
            self.v.validate(doc)

    def test_content_type_vide(self):
        doc = make_valid_doc(content_type="")
        with pytest.raises(MissingRequiredFieldError):
            self.v.validate(doc)


class TestContentValidator:
    def setup_method(self):
        self.v = ContentValidator(max_size_bytes=1024)

    def test_doc_valide_passe(self):
        self.v.validate(make_valid_doc())

    def test_contenu_vide_leve_empty_error(self):
        doc = make_valid_doc(content=b"", size_bytes=0)
        with pytest.raises(EmptyContentError):
            self.v.validate(doc)

    def test_taille_depassee(self):
        doc = RawDocument.create(
            instance_id="inst-1", connector_id="rss",
            uri="https://example.com/large",
            content=b"x" * 2048,
            content_type="text/plain",
        )
        v = ContentValidator(max_size_bytes=512)
        with pytest.raises(SizeLimitError):
            v.validate(doc)

    def test_checksum_altere(self):
        doc = make_valid_doc(checksum="0" * 64)
        with pytest.raises(ChecksumMismatchError):
            self.v.validate(doc)


class TestCompositeValidator:
    def test_valide_si_tous_passent(self):
        v = CompositeValidator([SchemaValidator(), ContentValidator()])
        v.validate(make_valid_doc())

    def test_stoppe_au_premier_echec(self):
        v = CompositeValidator([SchemaValidator(), ContentValidator()])
        doc = make_valid_doc(content=b"", size_bytes=0)
        with pytest.raises(EmptyContentError):
            v.validate(doc)

    def test_default_validator_passe_sur_doc_valide(self):
        v = default_validator()
        v.validate(make_valid_doc())
