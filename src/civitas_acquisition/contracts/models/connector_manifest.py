"""
ConnectorManifest — self-description d'un connecteur.

Chaque connecteur déclare ses capacités, contraintes et prérequis via son manifest.
Le Registry utilise les manifests pour router, découvrir et valider les connecteurs
SANS les instancier ni faire le moindre appel réseau.

C'est le contrat fondamental qui rend le système auto-documenté et extensible.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class ChannelType(Enum):
    """Les 6 modes d'entrée de données dans la plateforme."""
    POLLING   = auto()   # Pull périodique planifié
    WEBHOOK   = auto()   # Push inbound déclenché par la source
    STREAMING = auto()   # Consommation continue (Kafka, Kinesis)
    QUEUE     = auto()   # File de messages (AMQP, SQS)
    FILE_DROP = auto()   # Surveillance de répertoire ou bucket
    MANUAL    = auto()   # Déclenchement one-shot par opérateur


class SourceCategory(Enum):
    """Catégorie fonctionnelle de la source."""
    CLOUD_STORAGE   = auto()
    DATABASE        = auto()
    COLLABORATION   = auto()
    CODE_REPOSITORY = auto()
    COMMUNICATION   = auto()
    WEB             = auto()
    STREAMING       = auto()
    CUSTOM          = auto()


@dataclass(frozen=True)
class RateLimit:
    """Contrainte de débit déclarée par le connecteur."""
    requests_per_second: float
    burst_size: int


@dataclass(frozen=True)
class CredentialSpec:
    """
    Spécification d'un credential requis par un connecteur.
    sensitive=True signifie que la valeur ne doit jamais être loggée
    et doit être récupérée depuis le Vault au runtime.
    """
    key: str
    description: str
    required: bool = True
    sensitive: bool = True


@dataclass(frozen=True)
class ConnectorManifest:
    """
    Carte d'identité complète et immuable d'un connecteur.

    Un connecteur DOIT implémenter manifest() comme méthode synchrone,
    sans état, sans appel réseau. Le Registry appelle manifest() au moment
    de la découverte, avant toute connexion.
    """

    # Identité
    connector_id: str         # Identifiant unique : "github", "notion", "s3"
    display_name: str         # Nom lisible : "GitHub"
    version: str              # semver : "1.2.0"

    # Classification
    source_category: SourceCategory
    supported_channels: frozenset[ChannelType]
    supported_mime_types: frozenset[str]   # "*/*" = accepte tout

    # Credentials
    required_credentials: tuple[CredentialSpec, ...]
    optional_credentials: tuple[CredentialSpec, ...] = ()

    # Contraintes opérationnelles
    rate_limit: Optional[RateLimit] = None
    max_batch_size: int = 100
    max_concurrency: int = 1

    # Capacités
    supports_cursor: bool = True       # Peut reprendre depuis un checkpoint
    supports_delta: bool = False       # Peut ne retourner que les nouveautés
    supports_streaming: bool = False   # Peut émettre en continu
    supports_discovery: bool = True    # Peut lister les ressources disponibles

    # ── Méthodes utilitaires ──────────────────────────────────────────────────

    def supports_channel(self, channel: ChannelType) -> bool:
        return channel in self.supported_channels

    def supports_mime_type(self, mime_type: str) -> bool:
        if "*/*" in self.supported_mime_types:
            return True
        major_wildcard = mime_type.split("/")[0] + "/*"
        return (
            mime_type in self.supported_mime_types
            or major_wildcard in self.supported_mime_types
        )

    def all_credential_keys(self) -> tuple[str, ...]:
        """Tous les credential keys, requis + optionnels."""
        return (
            tuple(c.key for c in self.required_credentials)
            + tuple(c.key for c in self.optional_credentials)
        )

    def __str__(self) -> str:
        channels = ", ".join(c.name for c in self.supported_channels)
        return f"[{self.connector_id} v{self.version}] channels=[{channels}]"
