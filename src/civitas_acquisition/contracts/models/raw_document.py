"""
RawDocument — sortie canonique de la plateforme d'Acquisition.

C'est l'unique type que la plateforme d'Acquisition produit.
La Transformation Platform ne connaît que ce type.

Propriétés fondamentales :
- Immuable (frozen dataclass)
- ID déterministe : SHA-256(instance_id + uri)
  → le même document acquis deux fois produit le même ID
  → idempotence gratuite, déduplication triviale
- Checksum du contenu pour intégrité
- Métadonnées source verbatim (non transformées)
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from .cursor import Cursor


def make_document_id(instance_id: str, uri: str) -> str:
    """
    Génère un ID de document déterministe et idempotent.
    SHA-256(instance_id:uri) — 64 caractères hexadécimaux.

    Garantie : même source + même URI → même ID, toujours.
    """
    payload = f"{instance_id}:{uri}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def sha256_checksum(content: bytes) -> str:
    """SHA-256 du contenu brut — 64 caractères hexadécimaux."""
    return hashlib.sha256(content).hexdigest()


@dataclass(frozen=True)
class SourceReference:
    """Référence canonique vers l'origine d'un document."""
    connector_id: str    # "github", "notion", "s3", ...
    instance_id: str     # UUID de l'instance connecteur configurée
    uri: str             # URI canonique dans la source (URL, path, ID natif)
    version: Optional[str] = None  # ETag, SHA, numéro de séquence côté source


@dataclass(frozen=True)
class RawDocument:
    """
    Enveloppe immuable et standardisée pour tout document acquis.

    OUTPUT CONTRACT de la plateforme d'Acquisition.
    Toute source externe, quel que soit son format natif,
    produit un RawDocument à la sortie du pipeline.

    Ne jamais modifier ce modèle sans bump de version de schéma
    et migration des données existantes dans le Raw Repository.
    """

    id: str                               # SHA-256(instance_id:uri)
    source_ref: SourceReference
    content: bytes                        # Contenu brut — jamais transformé ici
    content_type: str                     # MIME type : "text/plain", "application/pdf", ...
    size_bytes: int
    checksum: str                         # SHA-256 du content
    acquired_at: datetime                 # UTC, moment de l'acquisition
    source_metadata: dict[str, Any]       # Métadonnées natives de la source, verbatim
    cursor: Optional[Cursor] = None       # Position dans la source après ce document
    tags: tuple[str, ...] = field(default_factory=tuple)

    # ── Factory method ────────────────────────────────────────────────────────

    @classmethod
    def create(
        cls,
        *,
        instance_id: str,
        connector_id: str,
        uri: str,
        content: bytes,
        content_type: str,
        source_metadata: dict[str, Any] | None = None,
        version: Optional[str] = None,
        cursor: Optional[Cursor] = None,
        tags: tuple[str, ...] = (),
    ) -> RawDocument:
        """
        Factory avec calcul automatique des champs dérivés (id, checksum, size, acquired_at).
        Préférer cette méthode au constructeur direct dans les connecteurs.
        """
        return cls(
            id=make_document_id(instance_id, uri),
            source_ref=SourceReference(
                connector_id=connector_id,
                instance_id=instance_id,
                uri=uri,
                version=version,
            ),
            content=content,
            content_type=content_type,
            size_bytes=len(content),
            checksum=sha256_checksum(content),
            acquired_at=datetime.now(tz=timezone.utc),
            source_metadata=source_metadata or {},
            cursor=cursor,
            tags=tags,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def is_text(self) -> bool:
        return self.content_type.startswith("text/")

    def is_binary(self) -> bool:
        return not self.is_text()

    def verify_integrity(self) -> bool:
        """Vérifie que le contenu n'a pas été altéré depuis l'acquisition."""
        return sha256_checksum(self.content) == self.checksum

    def __repr__(self) -> str:
        return (
            f"RawDocument("
            f"id={self.id[:12]}..., "
            f"connector={self.source_ref.connector_id!r}, "
            f"uri={self.source_ref.uri!r}, "
            f"size={self.size_bytes:,}B, "
            f"type={self.content_type!r})"
        )
