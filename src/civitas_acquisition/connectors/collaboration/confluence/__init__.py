from __future__ import annotations
def __getattr__(name):
    if name == "ConfluenceConnector":
        from civitas_acquisition.connectors.collaboration.confluence.connector import ConfluenceConnector
        return ConfluenceConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
__all__ = ["ConfluenceConnector"]
