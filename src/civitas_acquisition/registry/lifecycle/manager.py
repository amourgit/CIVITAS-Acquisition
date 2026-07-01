"""
ConnectorLifecycleManager — gestion complète du cycle de vie des connecteurs.

Remplace le simple ConnectorFactory.
Responsabilités :
  - Instanciation et connexion (create)
  - Surveillance de santé (health monitoring)
  - Suspension / reprise
  - Hot reload de configuration
  - Arrêt propre
  - Inventaire des instances actives

Toute interaction avec un connecteur passe par ce manager.
Aucun autre composant ne détient de référence directe à un connecteur instancié.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from civitas_acquisition.contracts.ports.connector_port import ConnectorPort
from civitas_acquisition.contracts.ports.vault_port import CredentialVaultPort
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.errors.connector_errors import ConnectorNotFoundError

logger = logging.getLogger(__name__)


class ManagedConnector:
    """Enveloppe d'une instance connecteur avec ses métadonnées de lifecycle."""

    def __init__(self, connector: ConnectorPort, config: ConnectorConfig) -> None:
        self.connector = connector
        self.config = config
        self.last_health: Optional[HealthStatus] = None
        self.suspended = False
        self._lock = asyncio.Lock()

    @property
    def instance_id(self) -> str:
        return self.config.instance_id

    @property
    def connector_id(self) -> str:
        return self.config.connector_id


class ConnectorLifecycleManager:
    """
    Gestionnaire centralisé du cycle de vie des instances connecteur.

    Usage :
        manager = ConnectorLifecycleManager(registry, vault)
        connector = await manager.start("github", "inst-1", options={...})
        await manager.health_check_all()
        await manager.reload("inst-1", new_config)
        await manager.stop("inst-1")
    """

    def __init__(
        self,
        registry: "ConnectorRegistry",  # type: ignore[name-defined]  # noqa: F821
        vault: CredentialVaultPort,
    ) -> None:
        self._registry = registry
        self._vault = vault
        self._active: dict[str, ManagedConnector] = {}

    async def start(
        self,
        connector_id: str,
        instance_id: str,
        options: dict | None = None,
    ) -> ConnectorPort:
        """
        Démarre une instance connecteur :
        1. Récupère la classe depuis le registry
        2. Résout les credentials depuis le vault
        3. Instancie et connecte
        4. Enregistre dans les instances actives
        """
        cls = self._registry.get(connector_id)
        connector: ConnectorPort = cls()
        manifest = connector.manifest()

        credentials: dict[str, str] = {}
        for spec in manifest.required_credentials:
            path = f"acquisition/{instance_id}/{spec.key}"
            secret = await self._vault.get_secret(path)
            credentials[spec.key] = secret.value

        config = ConnectorConfig(
            instance_id=instance_id,
            connector_id=connector_id,
            credentials=credentials,
            options=options or {},
        )

        await connector.connect(config)

        managed = ManagedConnector(connector, config)
        self._active[instance_id] = managed

        logger.info("Started connector instance: %s (%s)", instance_id, connector_id)
        return connector

    async def stop(self, instance_id: str) -> None:
        """Arrête proprement une instance et la retire des actives."""
        managed = self._active.pop(instance_id, None)
        if managed is None:
            return
        try:
            await managed.connector.disconnect()
            logger.info("Stopped connector instance: %s", instance_id)
        except Exception as e:
            logger.warning("Error stopping %s: %s", instance_id, e)

    async def stop_all(self) -> None:
        """Arrête toutes les instances actives."""
        instance_ids = list(self._active.keys())
        for instance_id in instance_ids:
            await self.stop(instance_id)

    async def suspend(self, instance_id: str) -> None:
        """Suspend une instance sans la déconnecter."""
        if managed := self._active.get(instance_id):
            managed.suspended = True
            logger.info("Suspended connector instance: %s", instance_id)

    async def resume(self, instance_id: str) -> None:
        """Reprend une instance suspendue."""
        if managed := self._active.get(instance_id):
            managed.suspended = False
            logger.info("Resumed connector instance: %s", instance_id)

    async def reload(self, instance_id: str, new_options: dict) -> ConnectorPort:
        """
        Hot reload d'une instance :
        arrête l'ancienne, démarre une nouvelle avec la nouvelle config.
        """
        old = self._active.get(instance_id)
        connector_id = old.config.connector_id if old else None
        if connector_id is None:
            raise ConnectorNotFoundError(instance_id)
        await self.stop(instance_id)
        return await self.start(connector_id, instance_id, options=new_options)

    async def health_check_all(self) -> dict[str, HealthStatus]:
        """Effectue un health check sur toutes les instances actives."""
        results: dict[str, HealthStatus] = {}
        for instance_id, managed in self._active.items():
            if managed.suspended:
                continue
            try:
                status = await managed.connector.healthcheck()
                managed.last_health = status
                results[instance_id] = status
            except Exception as e:
                status = HealthStatus.fail(str(e))
                managed.last_health = status
                results[instance_id] = status
        return results

    def get(self, instance_id: str) -> ConnectorPort:
        """Retourne une instance active. Lève KeyError si inexistante."""
        if instance_id not in self._active:
            raise KeyError(f"No active connector instance '{instance_id}'")
        return self._active[instance_id].connector

    def list_active(self) -> list[str]:
        return list(self._active.keys())

    def is_suspended(self, instance_id: str) -> bool:
        managed = self._active.get(instance_id)
        return managed.suspended if managed else False
