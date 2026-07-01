"""
ConnectorRegistry — registre central de tous les connecteurs disponibles.

Responsabilités :
  - Enregistrement manuel ou automatique (auto-discovery)
  - Requêtes par connector_id, channel_type, source_category
  - Validation du manifest au moment de l'enregistrement
  - Inventaire complet des capacités de la plateforme

Aucune instance connecteur ici. Que des classes et leurs manifests.
L'instanciation est du ressort du ConnectorLifecycleManager.
"""
from __future__ import annotations

import importlib
import logging
import pkgutil

from civitas_acquisition.contracts.ports.connector_port import ConnectorPort
from civitas_acquisition.contracts.models.connector_manifest import (
    ConnectorManifest,
    ChannelType,
    SourceCategory,
)
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorNotFoundError,
    ConnectorAlreadyRegisteredError,
    ManifestValidationError,
)

logger = logging.getLogger(__name__)


class ConnectorRegistry:
    """
    Registre central des connecteurs disponibles dans la plateforme.

    Usage :
        registry = ConnectorRegistry()
        registry.autodiscover("civitas_acquisition.connectors")
        cls = registry.get("github")
        manifest = registry.manifest("github")
        polling_connectors = registry.find_by_channel(ChannelType.POLLING)
    """

    def __init__(self) -> None:
        self._classes: dict[str, type[ConnectorPort]] = {}
        self._manifests: dict[str, ConnectorManifest] = {}

    def register(self, cls: type[ConnectorPort]) -> None:
        """
        Enregistre une classe connecteur.
        Crée une instance temporaire juste pour lire le manifest.
        Aucune connexion établie.
        """
        try:
            instance = object.__new__(cls)
            manifest = instance.manifest()
        except Exception as exc:
            raise ManifestValidationError(
                connector_id=cls.__name__,
                field="manifest()",
                reason=f"Cannot read manifest: {exc}",
            ) from exc

        self._validate_manifest(manifest)

        if manifest.connector_id in self._classes:
            existing = self._manifests[manifest.connector_id]
            if existing.version == manifest.version:
                logger.debug("Connector '%s' already registered — skipping", manifest.connector_id)
                return
            # Nouvelle version — on remplace
            logger.info(
                "Updating connector '%s' from v%s to v%s",
                manifest.connector_id, existing.version, manifest.version,
            )

        self._classes[manifest.connector_id] = cls
        self._manifests[manifest.connector_id] = manifest
        logger.debug("Registered connector: %s", manifest)

    def autodiscover(self, package: str) -> int:
        """
        Découvre et enregistre automatiquement tous les ConnectorPort
        dans un package Python (y compris sous-packages).
        Retourne le nombre de connecteurs nouvellement enregistrés.
        """
        try:
            pkg = importlib.import_module(package)
        except ImportError:
            logger.warning("Cannot autodiscover: package '%s' not found", package)
            return 0

        found = 0
        pkg_path = getattr(pkg, "__path__", [])
        for _, module_name, _ in pkgutil.walk_packages(pkg_path, package + "."):
            try:
                module = importlib.import_module(module_name)
            except Exception as exc:
                logger.debug("Skipping module '%s': %s", module_name, exc)
                continue

            for attr_name in dir(module):
                obj = getattr(module, attr_name, None)
                if (
                    isinstance(obj, type)
                    and issubclass(obj, ConnectorPort)
                    and obj is not ConnectorPort
                    and not getattr(obj, "__abstract__", False)
                    and not attr_name.startswith("_")
                    and not attr_name.endswith("Base")
                ):
                    before = len(self._classes)
                    try:
                        self.register(obj)
                        if len(self._classes) > before:
                            found += 1
                    except Exception as exc:
                        logger.warning("Failed to register %s: %s", attr_name, exc)

        logger.info("Autodiscovered %d connector(s) in '%s'", found, package)
        return found

    def get(self, connector_id: str) -> type[ConnectorPort]:
        if connector_id not in self._classes:
            raise ConnectorNotFoundError(
                connector_id=connector_id,
                available=list(self._classes.keys()),
            )
        return self._classes[connector_id]

    def manifest(self, connector_id: str) -> ConnectorManifest:
        if connector_id not in self._manifests:
            raise ConnectorNotFoundError(
                connector_id=connector_id,
                available=list(self._manifests.keys()),
            )
        return self._manifests[connector_id]

    def find_by_channel(self, channel: ChannelType) -> list[ConnectorManifest]:
        return [m for m in self._manifests.values() if channel in m.supported_channels]

    def find_by_category(self, category: SourceCategory) -> list[ConnectorManifest]:
        return [m for m in self._manifests.values() if m.source_category == category]

    def list_all(self) -> list[ConnectorManifest]:
        return list(self._manifests.values())

    def is_registered(self, connector_id: str) -> bool:
        return connector_id in self._classes

    def count(self) -> int:
        return len(self._classes)

    def _validate_manifest(self, manifest: ConnectorManifest) -> None:
        if not manifest.connector_id:
            raise ManifestValidationError("?", "connector_id", "must not be empty")
        if not manifest.version:
            raise ManifestValidationError(manifest.connector_id, "version", "must not be empty")
        if not manifest.supported_channels:
            raise ManifestValidationError(
                manifest.connector_id, "supported_channels", "must support at least one channel"
            )
