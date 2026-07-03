"""Slack connector — lazy imports."""
from __future__ import annotations

def __getattr__(name: str):
    if name == "SlackConnector":
        from civitas_acquisition.connectors.communication.slack.connector import SlackConnector
        return SlackConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["SlackConnector"]
