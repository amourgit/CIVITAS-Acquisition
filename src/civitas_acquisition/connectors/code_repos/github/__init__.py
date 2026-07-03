"""
GitHub connector — lazy imports pour éviter les dépendances au niveau module.
Importer directement depuis les sous-modules si aiohttp n'est pas installé.
"""
from __future__ import annotations
from typing import TYPE_CHECKING

# Imports non-réseau : toujours disponibles
from civitas_acquisition.connectors.code_repos.github.webhook import (
    GitHubWebhookParser, WebhookEvent,
)
from civitas_acquisition.connectors.code_repos.github.webhook_manager import (
    GitHubWebhookManager, WebhookRegistry, RegisteredWebhook,
)
from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth


def __getattr__(name: str):
    """Lazy import des composants qui dépendent de aiohttp."""
    if name == "GitHubConnector":
        from civitas_acquisition.connectors.code_repos.github.connector import GitHubConnector
        return GitHubConnector
    if name == "GitHubClient":
        from civitas_acquisition.connectors.code_repos.github.client import GitHubClient
        return GitHubClient
    if name == "GitHubOperations":
        from civitas_acquisition.connectors.code_repos.github.operations import GitHubOperations
        return GitHubOperations
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "GitHubConnector", "GitHubClient", "GitHubOperations",
    "GitHubWebhookParser", "WebhookEvent",
    "GitHubWebhookManager", "WebhookRegistry", "RegisteredWebhook",
    "GitHubAuth",
]
