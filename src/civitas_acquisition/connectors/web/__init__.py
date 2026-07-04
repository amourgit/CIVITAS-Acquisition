from __future__ import annotations
def __getattr__(name):
    if name == "WebConnector":
        from civitas_acquisition.connectors.web.connector import WebConnector
        return WebConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
__all__ = ["WebConnector"]
