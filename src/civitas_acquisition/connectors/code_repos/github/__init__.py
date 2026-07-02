"""GitHub Connector — CIVITAS Acquisition Platform."""
from .connector import GitHubConnector
from .webhook import GitHubWebhookParser, WebhookEvent
__all__ = ["GitHubConnector", "GitHubWebhookParser", "WebhookEvent"]
