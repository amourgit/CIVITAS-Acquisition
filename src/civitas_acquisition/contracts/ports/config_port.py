"""
ConfigPort — interface abstraite de la gestion de configuration.

La configuration de la plateforme est complexe :
  - Connecteurs : credentials, options, rate limits par instance
  - Channels : stratégies, intervalles, timeouts
  - Workers : pool size, timeouts
  - Policies : retry, backpressure, throttling

Et elle doit supporter :
  - Hot reload (modification sans redémarrage)
  - Validation à la lecture (fail fast)
  - Watchers (notification des composants en cas de changement)

Implémentations prévues : YAML/TOML fichier, PostgreSQL, etcd, Consul.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ConfigSection:
    """Section de configuration extraite du loader."""
    key: str
    data: dict[str, Any]
    version: str | None = None   # Pour détecter les changements

    def get(self, path: str, default: Any = None) -> Any:
        """
        Accès par chemin pointé : "connectors.github.rate_limit".
        Retourne default si le chemin n'existe pas.
        """
        parts = path.split(".")
        current: Any = self.data
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def require(self, path: str) -> Any:
        """Comme get() mais lève KeyError si absent."""
        result = self.get(path)
        if result is None:
            raise KeyError(f"Required config key '{path}' not found in section '{self.key}'")
        return result


ConfigChangeHandler = Callable[[ConfigSection], None]


class ConfigPort(ABC):
    """
    Interface abstraite pour le chargement et la surveillance de configuration.
    """

    @abstractmethod
    async def load(self, section: str) -> ConfigSection:
        """
        Charge une section de configuration.
        Lève ConfigNotFoundError si la section n'existe pas.
        Lève ConfigValidationError si le contenu est invalide.
        """
        ...

    @abstractmethod
    async def load_all(self) -> dict[str, ConfigSection]:
        """Charge toutes les sections disponibles."""
        ...

    @abstractmethod
    def watch(self, section: str, handler: ConfigChangeHandler) -> "ConfigWatchHandle":
        """
        Surveille une section et notifie le handler en cas de changement.
        Retourne un handle pour arrêter la surveillance.
        """
        ...

    @abstractmethod
    async def reload(self, section: str) -> ConfigSection:
        """Force le rechargement d'une section depuis la source."""
        ...


@dataclass
class ConfigWatchHandle:
    """Handle pour stopper une surveillance de configuration."""
    section: str
    watch_id: str
    _stop_fn: Callable[[], None] | None = None

    def stop(self) -> None:
        if self._stop_fn:
            self._stop_fn()
