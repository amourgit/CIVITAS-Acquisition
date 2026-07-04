from __future__ import annotations
def __getattr__(name):
    if name == "GmailConnector":
        from civitas_acquisition.connectors.communication.gmail.connector import GmailConnector
        return GmailConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
__all__ = ["GmailConnector"]
