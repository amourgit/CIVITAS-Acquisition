"""Tests pour PostgreSQLConnector — sans connexion réelle."""
import pytest
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from civitas_acquisition.connectors.databases.postgres.connector import PostgreSQLConnector
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType


def make_config(**options) -> ConnectorConfig:
    return ConnectorConfig(
        instance_id="inst-pg-1",
        connector_id="postgresql",
        credentials={
            "host": "localhost", "database": "mydb",
            "username": "civitas", "password": "secret",
        },
        options={"tables": ["users", "orders"], "schema": "public", **options},
    )


def make_connector(options: dict | None = None) -> PostgreSQLConnector:
    config = make_config(**(options or {}))
    connector = PostgreSQLConnector()
    connector._connected = True
    connector._config = config
    connector._schema = config.get_option("schema", "public")
    connector._tables = config.get_option("tables", [])
    connector._cursor_col = config.get_option("cursor_column", "updated_at")
    connector._batch_size = config.get_option("batch_size", 1000)
    connector._max_rows = config.get_option("max_rows", 100_000)
    connector._mode = config.get_option("mode", "snapshot")
    connector._include_schema = config.get_option("include_schema", True)
    connector._row_format = config.get_option("row_format", "json")
    connector._replication_slot = config.get_option("replication_slot", "civitas_slot")
    connector._publication = config.get_option("publication", "civitas_publication")
    return connector


class TestManifest:
    def test_connector_id(self):
        assert PostgreSQLConnector().manifest().connector_id == "postgresql"

    def test_supported_channels(self):
        m = PostgreSQLConnector().manifest()
        assert ChannelType.POLLING in m.supported_channels
        assert ChannelType.STREAMING in m.supported_channels
        assert ChannelType.MANUAL in m.supported_channels

    def test_credentials_requis(self):
        keys = [c.key for c in PostgreSQLConnector().manifest().required_credentials]
        assert "host" in keys
        assert "database" in keys
        assert "username" in keys
        assert "password" in keys

    def test_supports_cursor_et_delta(self):
        m = PostgreSQLConnector().manifest()
        assert m.supports_cursor is True
        assert m.supports_delta is True


class TestBuildDsn:
    def test_dsn_complet(self):
        config = make_config()
        dsn = PostgreSQLConnector._build_dsn(config)
        assert "localhost" in dsn
        assert "mydb" in dsn
        assert "civitas" in dsn
        assert "postgresql://" in dsn

    def test_dsn_avec_sslmode(self):
        config = make_config()
        config = ConnectorConfig(
            instance_id="inst-1", connector_id="postgresql",
            credentials={
                "host": "pg.example.com", "database": "prod",
                "username": "admin", "password": "pass",
                "sslmode": "verify-full", "port": "5433",
            },
        )
        dsn = PostgreSQLConnector._build_dsn(config)
        assert "verify-full" in dsn
        assert "5433" in dsn


class TestMakeSerializable:
    def test_datetime_converti(self):
        connector = make_connector()
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = connector._make_serializable({"created_at": dt, "name": "Alice"})
        assert result["created_at"] == "2024-01-15T12:00:00+00:00"
        assert result["name"] == "Alice"

    def test_bytes_remplace(self):
        connector = make_connector()
        result = connector._make_serializable({"data": b"\x00\x01\x02", "id": 1})
        assert result["data"] == "<binary>"
        assert result["id"] == 1

    def test_valeurs_normales_inchangees(self):
        connector = make_connector()
        row = {"id": 42, "name": "Alice", "score": 9.5, "active": True}
        result = connector._make_serializable(row)
        assert result == row


class TestExtractPk:
    def test_colonne_id(self):
        connector = make_connector()
        row = {"id": 42, "name": "Alice"}
        schema = {"primary_keys": [], "columns": {}}
        assert connector._extract_pk(row, schema) == "42"

    def test_cle_primaire_explicite(self):
        connector = make_connector()
        row = {"user_id": 99, "email": "alice@example.com"}
        schema = {"primary_keys": ["user_id"], "columns": {}}
        assert connector._extract_pk(row, schema) == "99"

    def test_pk_composite(self):
        connector = make_connector()
        row = {"order_id": 1, "product_id": 5, "qty": 3}
        schema = {"primary_keys": ["order_id", "product_id"], "columns": {}}
        assert connector._extract_pk(row, schema) == "1|5"


class TestMapRow:
    def test_map_row_basique(self):
        connector = make_connector()
        row = {"id": 1, "name": "Alice", "email": "alice@example.com",
               "updated_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)}
        schema = {"primary_keys": ["id"], "columns": {"id": {"type": "integer"}}}
        doc = connector._map_row(row, "users", schema)

        assert doc.content_type == "application/json"
        payload = json.loads(doc.content)
        assert payload["id"] == 1
        assert payload["name"] == "Alice"
        assert "row" in doc.tags
        assert "table:users" in doc.tags

    def test_map_row_uri_unique_par_pk(self):
        connector = make_connector()
        schema = {"primary_keys": ["id"], "columns": {}}
        row1 = {"id": 1, "name": "Alice"}
        row2 = {"id": 2, "name": "Bob"}
        doc1 = connector._map_row(row1, "users", schema)
        doc2 = connector._map_row(row2, "users", schema)
        assert doc1.source_ref.uri != doc2.source_ref.uri
        assert doc1.id != doc2.id

    def test_map_row_source_metadata(self):
        connector = make_connector()
        schema = {"primary_keys": ["id"], "columns": {}}
        row = {"id": 5, "value": "test"}
        doc = connector._map_row(row, "products", schema)
        assert doc.source_metadata["table"] == "products"
        assert doc.source_metadata["schema"] == "public"
        assert doc.source_metadata["resource_type"] == "table_row"

    def test_map_row_version_depuis_cursor_col(self):
        connector = make_connector()
        schema = {"primary_keys": ["id"], "columns": {}}
        dt = datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
        row = {"id": 1, "name": "Alice", "updated_at": dt}
        doc = connector._map_row(row, "users", schema)
        assert doc.source_ref.version == dt.isoformat()


class TestStamp:
    def test_stamp_ajoute_cursor_composite(self):
        connector = make_connector()
        from civitas_acquisition.contracts.models.raw_document import RawDocument
        doc = RawDocument.create(
            instance_id="inst-pg-1", connector_id="postgresql",
            uri="postgresql://public/users/1",
            content=b'{"id":1}', content_type="application/json",
        )
        cursors = {"public.users": "2024-01-20T00:00:00", "public.orders": "2024-01-18T00:00:00"}
        stamped = connector._stamp(doc, cursors)
        assert stamped.cursor is not None
        parsed = json.loads(stamped.cursor.value)
        assert parsed["public.users"] == "2024-01-20T00:00:00"
        assert stamped.cursor.source_type == "token"


class TestParsePgoutput:
    def test_parse_json_valide(self):
        connector = make_connector()
        data = json.dumps({
            "action": "I", "table": "users",
            "new": {"id": 42, "name": "Alice"}
        })
        doc = connector._parse_pgoutput(data, "0/15D0000")
        assert doc is not None
        assert "cdc" in doc.tags
        assert "table:users" in doc.tags
        assert doc.source_metadata["action"] == "I"

    def test_parse_donnees_brutes(self):
        connector = make_connector()
        doc = connector._parse_pgoutput("BEGIN 123", "0/15D0001")
        assert doc is not None
        assert doc.content_type == "application/json"

    def test_cursor_lsn(self):
        connector = make_connector()
        data = json.dumps({"action": "U", "table": "orders"})
        doc = connector._parse_pgoutput(data, "0/1A2B3C4D")
        assert doc.cursor.source_type == "sequence"
        assert doc.cursor.value == "0/1A2B3C4D"
