"""
ConnectorConfig — configuration runtime résolue par la Factory.

Créé par la ConnectorFactory après résolution des credentials depuis le Vault.
Les credentials ne sont jamais loggés ni sérialisés.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class ConnectorConfig:
    """
    Configuration runtime d'une instance connecteur.

    instance_id : identifiant unique de cette instance (UUID).
                  Plusieurs instances du même connecteur peuvent coexister
                  (ex: deux repos GitHub différents).
    connector_id : référence au ConnectorManifest.connector_id.
    credentials : résolu depuis le Vault — sensible, jamais loggé.
    options : paramètres spécifiques au connecteur (timeouts, filtres, ...).
    workspace_id : support multi-tenant.
    """

    instance_id: str
    connector_id: str
    credentials: dict[str, str]
    options: dict[str, Any] = field(default_factory=dict)
    workspace_id: Optional[str] = None

    def get_credential(self, key: str) -> str:
        """Récupère un credential requis. Lève KeyError si absent."""
        if key not in self.credentials:
            raise KeyError(
                f"Credential '{key}' missing from config for instance '{self.instance_id}'"
            )
        return self.credentials[key]

    def get_option(self, key: str, default: Any = None) -> Any:
        return self.options.get(key, default)

    def __repr__(self) -> str:
        """Les credentials ne sont JAMAIS exposés dans le repr."""
        return (
            f"ConnectorConfig("
            f"instance_id={self.instance_id!r}, "
            f"connector_id={self.connector_id!r}, "
            f"credentials=<{len(self.credentials)} keys redacted>, "
            f"workspace_id={self.workspace_id!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()
