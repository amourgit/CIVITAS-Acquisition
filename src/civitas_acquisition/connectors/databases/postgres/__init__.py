from __future__ import annotations
def __getattr__(name: str):
    if name == "PostgreSQLConnector":
        from civitas_acquisition.connectors.databases.postgres.connector import PostgreSQLConnector
        return PostgreSQLConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
__all__ = ["PostgreSQLConnector"]
