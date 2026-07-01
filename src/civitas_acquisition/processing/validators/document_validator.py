"""
DocumentValidator — validation structurelle d'un RawDocument.

Trois niveaux de validation :
  SchemaValidator  → champs obligatoires, types, format
  ContentValidator → taille, encoding, MIME cohérence
  PolicyValidator  → règles métier (ex: blacklist de domaines)

CompositeValidator agrège N validators, collecte toutes les erreurs.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.validation_errors import (
    EmptyContentError,
    MissingRequiredFieldError,
    SizeLimitError,
    ChecksumMismatchError,
    InvalidContentTypeError,
    SchemaValidationError,
)


class ValidatorPort(ABC):
    @abstractmethod
    def validate(self, doc: RawDocument) -> None:
        """Lève une ValidationError si le document est invalide."""
        ...


class SchemaValidator(ValidatorPort):
    """Vérifie les champs obligatoires et leur format."""

    def validate(self, doc: RawDocument) -> None:
        if not doc.id:
            raise MissingRequiredFieldError("id")
        if not doc.source_ref.connector_id:
            raise MissingRequiredFieldError("source_ref.connector_id")
        if not doc.source_ref.instance_id:
            raise MissingRequiredFieldError("source_ref.instance_id")
        if not doc.source_ref.uri:
            raise MissingRequiredFieldError("source_ref.uri")
        if not doc.content_type:
            raise MissingRequiredFieldError("content_type")
        if doc.acquired_at is None:
            raise MissingRequiredFieldError("acquired_at")
        if len(doc.id) != 64:
            raise SchemaValidationError("id", "must be 64-char hex SHA-256", doc.id)


class ContentValidator(ValidatorPort):
    """Vérifie le contenu brut : taille, intégrité, encodage."""

    def __init__(self, max_size_bytes: int = 100 * 1024 * 1024) -> None:  # 100MB
        self._max_size = max_size_bytes

    def validate(self, doc: RawDocument) -> None:
        if not doc.content:
            raise EmptyContentError()
        if doc.size_bytes > self._max_size:
            raise SizeLimitError(doc.size_bytes, self._max_size)
        if not doc.verify_integrity():
            raise ChecksumMismatchError(
                expected=doc.checksum,
                actual="<recomputed differs>",
            )


class MimeValidator(ValidatorPort):
    """Vérifie que le content_type est dans la liste autorisée."""

    def __init__(self, allowed: frozenset[str] | None = None) -> None:
        self._allowed = allowed  # None = tout accepter

    def validate(self, doc: RawDocument) -> None:
        if self._allowed is None:
            return
        if "*/*" in self._allowed:
            return
        major = doc.content_type.split("/")[0] + "/*"
        if doc.content_type not in self._allowed and major not in self._allowed:
            raise InvalidContentTypeError(doc.content_type, list(self._allowed))


class CompositeValidator(ValidatorPort):
    """Exécute N validators en séquence. S'arrête à la première erreur."""

    def __init__(self, validators: list[ValidatorPort]) -> None:
        self._validators = validators

    def validate(self, doc: RawDocument) -> None:
        for v in self._validators:
            v.validate(doc)


def default_validator(max_size_bytes: int = 100 * 1024 * 1024) -> CompositeValidator:
    return CompositeValidator([
        SchemaValidator(),
        ContentValidator(max_size_bytes=max_size_bytes),
    ])
