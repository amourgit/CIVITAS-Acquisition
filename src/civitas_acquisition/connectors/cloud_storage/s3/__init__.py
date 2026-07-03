from __future__ import annotations

def __getattr__(name: str):
    if name == "S3Connector":
        from civitas_acquisition.connectors.cloud_storage.s3.connector import S3Connector
        return S3Connector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["S3Connector"]
