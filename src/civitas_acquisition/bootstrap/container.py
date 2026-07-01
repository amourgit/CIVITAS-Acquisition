"""
AcquisitionContainer — conteneur IoC de la plateforme d'acquisition.

Responsabilité unique : câbler toutes les dépendances.
Aucune logique métier ici. Que de l'assemblage.

Principe :
  - Toute dépendance est créée EXACTEMENT une fois (singleton)
  - L'ordre de construction respecte le graphe de dépendances
  - Aucun composant ne crée ses propres dépendances (DI stricte)
  - Le container est la seule classe autorisée à connaître TOUS les types concrets

Usage :
    container = AcquisitionContainer.from_config(config_dir="/etc/civitas")
    await container.start()
    # ...
    await container.stop()
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class AcquisitionContainer:
    """
    Conteneur d'injection de dépendances pour la plateforme d'acquisition.

    Instanciation unique par processus. Partagé via injection,
    jamais via singleton global.
    """

    def __init__(self) -> None:
        # Infrastructure runtime
        self._event_bus: Optional[object] = None
        self._metrics: Optional[object] = None
        self._vault: Optional[object] = None
        self._config: Optional[object] = None

        # Resilience
        self._retry_engine: Optional[object] = None
        self._circuit_breaker: Optional[object] = None
        self._dlq: Optional[object] = None

        # Registry & Lifecycle
        self._registry: Optional[object] = None
        self._lifecycle_manager: Optional[object] = None

        # Storage
        self._raw_repository: Optional[object] = None

        # Processing
        self._validator: Optional[object] = None
        self._deduplicator: Optional[object] = None
        self._pipeline: Optional[object] = None

        # Workers
        self._dispatcher: Optional[object] = None
        self._worker_pool: Optional[object] = None

        # Execution
        self._execution_engine: Optional[object] = None

        self._started = False

    @classmethod
    def for_development(cls, data_dir: str = "/tmp/civitas-dev") -> AcquisitionContainer:
        """
        Crée un container précâblé pour le développement local.
        Utilise les implémentations in-memory et filesystem.
        Sans vault réel, sans monitoring externe.
        """
        container = cls()
        container._wire_development(data_dir)
        return container

    def _wire_development(self, data_dir: str) -> None:
        """Câblage complet pour le développement."""
        from civitas_acquisition.runtime.eventbus.in_process import InProcessEventBus
        from civitas_acquisition.runtime.telemetry.metrics import AcquisitionMetrics
        from civitas_acquisition.runtime.resilience.dlq import InMemoryDLQ
        from civitas_acquisition.policies.retry_policy import STANDARD

        self._event_bus = InProcessEventBus()
        self._metrics = AcquisitionMetrics()
        self._dlq = InMemoryDLQ()

        # Le vault sera une implémentation .env en dev
        # self._vault = EnvVault()

        # Storage local
        from civitas_acquisition.storage.local import LocalRawRepository
        self._raw_repository = LocalRawRepository(base_dir=data_dir)

        logger.info("AcquisitionContainer wired for development (data_dir=%s)", data_dir)

    async def start(self) -> None:
        """Démarre tous les composants dans le bon ordre."""
        if self._started:
            logger.warning("Container already started")
            return
        # Démarrer le worker pool
        if self._worker_pool:
            await self._worker_pool.start()  # type: ignore[union-attr]
        self._started = True
        logger.info("AcquisitionContainer started")

    async def stop(self) -> None:
        """Arrête tous les composants dans l'ordre inverse."""
        if not self._started:
            return
        if self._worker_pool:
            await self._worker_pool.stop()  # type: ignore[union-attr]
        if self._lifecycle_manager:
            await self._lifecycle_manager.stop_all()  # type: ignore[union-attr]
        self._started = False
        logger.info("AcquisitionContainer stopped")

    # ── Accesseurs publics ────────────────────────────────────────────────────
    # Les autres composants reçoivent ces dépendances via injection
    # dans leur constructeur — jamais via container.get()

    @property
    def event_bus(self) -> object:
        assert self._event_bus, "EventBus not initialized"
        return self._event_bus

    @property
    def metrics(self) -> object:
        assert self._metrics, "Metrics not initialized"
        return self._metrics

    @property
    def raw_repository(self) -> object:
        assert self._raw_repository, "RawRepository not initialized"
        return self._raw_repository

    @property
    def dlq(self) -> object:
        assert self._dlq, "DLQ not initialized"
        return self._dlq
