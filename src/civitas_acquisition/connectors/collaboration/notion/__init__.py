"""Notion connector — lazy imports."""
from __future__ import annotations

def __getattr__(name: str):
    if name == "NotionConnector":
        from civitas_acquisition.connectors.collaboration.notion.connector import NotionConnector
        return NotionConnector
    if name == "NotionClient":
        from civitas_acquisition.connectors.collaboration.notion.client import NotionClient
        return NotionClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["NotionConnector", "NotionClient"]
