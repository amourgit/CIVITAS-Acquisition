"""
DiscoveryResult — liste des ressources disponibles dans une source.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DiscoveryResult:
    """
    Résultat de ConnectorPort.discover().
    Liste les ressources navigables dans la source (repos, pages, tables, buckets...).
    """

    resources: tuple[str, ...]   # URIs, chemins, ou IDs natifs de la source
    total: int                   # Nombre total (peut dépasser len(resources) si tronqué)
    metadata: dict[str, Any] = field(default_factory=dict)
    truncated: bool = False      # True si la source a plus de ressources que retourné

    def __len__(self) -> int:
        return len(self.resources)

    def is_empty(self) -> bool:
        return self.total == 0

    def __repr__(self) -> str:
        suffix = " (truncated)" if self.truncated else ""
        return f"DiscoveryResult(total={self.total}, returned={len(self.resources)}{suffix})"
