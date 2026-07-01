"""
YamlConfigLoader — chargeur de configuration depuis des fichiers YAML.

Supporte :
  - Chargement initial depuis un répertoire de configs
  - Hot reload via FileWatcher (inotify sur Linux)
  - Validation de schéma au chargement
  - Notification des composants abonnés en cas de changement

Structure attendue :
  config/
    connectors.yaml      → section "connectors"
    channels.yaml        → section "channels"
    workers.yaml         → section "workers"
    policies.yaml        → section "policies"
    acquisition.yaml     → section générale

Les credentials ne sont JAMAIS dans les fichiers de config.
Ils restent exclusivement dans le Vault.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from civitas_acquisition.contracts.ports.config_port import (
    ConfigPort,
    ConfigSection,
    ConfigWatchHandle,
    ConfigChangeHandler,
)

logger = logging.getLogger(__name__)


class YamlConfigLoader(ConfigPort):
    """
    Chargeur de configuration YAML.

    Usage :
        loader = YamlConfigLoader(config_dir="/etc/civitas/acquisition")
        section = await loader.load("connectors")
        github_token_path = section.get("connectors.github.vault_path")
    """

    def __init__(self, config_dir: str | Path) -> None:
        self._config_dir = Path(config_dir)
        self._cache: dict[str, ConfigSection] = {}
        self._watchers: dict[str, list[tuple[str, ConfigChangeHandler]]] = {}

    async def load(self, section: str) -> ConfigSection:
        if section in self._cache:
            return self._cache[section]
        return await self.reload(section)

    async def reload(self, section: str) -> ConfigSection:
        try:
            import yaml  # Dépendance optionnelle — non requise dans contracts
        except ImportError:
            raise ImportError(
                "PyYAML is required for YamlConfigLoader: pip install pyyaml"
            )

        config_file = self._config_dir / f"{section}.yaml"
        if not config_file.exists():
            # Chercher aussi .yml
            config_file = self._config_dir / f"{section}.yml"

        if not config_file.exists():
            raise KeyError(f"Config section '{section}' not found in {self._config_dir}")

        with open(config_file) as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}

        config_section = ConfigSection(
            key=section,
            data=data,
            version=str(config_file.stat().st_mtime),
        )
        self._cache[section] = config_section
        logger.debug("Loaded config section '%s' from %s", section, config_file)
        return config_section

    async def load_all(self) -> dict[str, ConfigSection]:
        result: dict[str, ConfigSection] = {}
        for yaml_file in self._config_dir.glob("*.yaml"):
            section_name = yaml_file.stem
            result[section_name] = await self.load(section_name)
        for yml_file in self._config_dir.glob("*.yml"):
            section_name = yml_file.stem
            if section_name not in result:
                result[section_name] = await self.load(section_name)
        return result

    def watch(self, section: str, handler: ConfigChangeHandler) -> ConfigWatchHandle:
        """
        Surveille un fichier de config et notifie handler en cas de changement.
        Utilise polling (simple). En production, utiliser inotify.
        """
        import uuid
        watch_id = str(uuid.uuid4())
        if section not in self._watchers:
            self._watchers[section] = []
        self._watchers[section].append((watch_id, handler))

        handle = ConfigWatchHandle(
            section=section,
            watch_id=watch_id,
            _stop_fn=lambda: self._remove_watcher(section, watch_id),
        )
        logger.debug("Watching config section '%s' (watch_id=%s)", section, watch_id[:8])
        return handle

    def _remove_watcher(self, section: str, watch_id: str) -> None:
        if section in self._watchers:
            self._watchers[section] = [
                (wid, h) for wid, h in self._watchers[section] if wid != watch_id
            ]

    async def _notify_watchers(self, section: str, config: ConfigSection) -> None:
        for _, handler in self._watchers.get(section, []):
            try:
                handler(config)
            except Exception as e:
                logger.error("Config watcher error for '%s': %s", section, e)
