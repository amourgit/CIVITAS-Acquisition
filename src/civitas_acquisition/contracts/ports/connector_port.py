"""
ConnectorPort — interface abstraite pour tous les connecteurs source.

C'est LE contrat central de la plateforme.
Le pipeline, le registry et la factory ne connaissent QUE cette interface.
Jamais les classes concrètes.

Règles pour les implémenteurs :
1. manifest() doit être synchrone, sans état, sans appel réseau.
2. pull() doit être un async generator (yield, pas return).
3. Le curseur est fourni en argument, jamais stocké dans self.
4. Toutes les exceptions doivent être des sous-classes d'AcquisitionError.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AsyncIterator, Optional

from ..models.connector_manifest import ConnectorManifest
from ..models.connector_config import ConnectorConfig
from ..models.cursor import Cursor
from ..models.raw_document import RawDocument
from ..models.health_status import HealthStatus
from ..models.discovery_result import DiscoveryResult


class ConnectorPort(ABC):
    """
    Interface abstraite pour tous les connecteurs source de la plateforme.

    Une implémentation concrète représente UNE source de données
    (GitHub, Notion, S3, PostgreSQL, Slack, ...).

    Cycle de vie d'une instance :
        registry.get(connector_id)
            → factory.create(instance_id, config)
                → connector.connect(config)
                    → connector.healthcheck()
                    → connector.discover()
                    → connector.pull(cursor)
                → connector.disconnect()
    """

    @abstractmethod
    def manifest(self) -> ConnectorManifest:
        """
        Retourne la description statique du connecteur.

        DOIT être :
        - synchrone (pas async)
        - sans état (même résultat à chaque appel)
        - sans appel réseau
        - appelable avant connect()

        Appelé par le Registry au moment de la découverte.
        """
        ...

    @abstractmethod
    async def connect(self, config: ConnectorConfig) -> None:
        """
        Établit la connexion à la source externe.
        Appelé une seule fois avant pull().
        Lève ConnectorAuthenticationError si les credentials sont invalides.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Libère toutes les ressources réseau.
        Doit être idempotent (appelable plusieurs fois sans erreur).
        """
        ...

    @abstractmethod
    async def healthcheck(self) -> HealthStatus:
        """
        Sonde la disponibilité de la source.
        Doit être léger — pas de récupération de données complète.
        Typiquement : ping ou requête minimale avec timeout court.
        """
        ...

    @abstractmethod
    async def discover(self) -> DiscoveryResult:
        """
        Liste les ressources navigables dans la source.
        Optionnel fonctionnellement mais recommandé pour les sources structurées.
        """
        ...

    @abstractmethod
    async def pull(
        self,
        cursor: Optional[Cursor] = None,
        batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        """
        Stream de RawDocuments depuis la source.

        cursor=None  → pull complet depuis le début
        cursor=<val> → delta pull depuis cette position

        DOIT être implémenté comme async generator (utiliser yield).
        Les documents doivent être émis dans un ordre stable et reproductible.
        Le curseur inclus dans chaque RawDocument doit être monotoniquement croissant.

        Exemple d'implémentation :
            async def pull(self, cursor=None, batch_size=100):
                async for item in self._client.list(since=cursor, limit=batch_size):
                    yield RawDocument.create(...)
        """
        ...
