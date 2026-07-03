"""Tests pour SlackConnector — sans appel réseau."""
import pytest
import json
import hashlib
import hmac
import time

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from civitas_acquisition.connectors.communication.slack.connector import SlackConnector
from civitas_acquisition.contracts.models.connector_manifest import ChannelType


class TestSlackManifest:
    def test_connector_id(self):
        assert SlackConnector().manifest().connector_id == "slack"

    def test_channels(self):
        m = SlackConnector().manifest()
        assert ChannelType.POLLING in m.supported_channels
        assert ChannelType.WEBHOOK in m.supported_channels

    def test_required_credential(self):
        keys = [c.key for c in SlackConnector().manifest().required_credentials]
        assert "bot_token" in keys

    def test_optional_signing_secret(self):
        keys = [c.key for c in SlackConnector().manifest().optional_credentials]
        assert "signing_secret" in keys


class TestWebhookVerification:
    SECRET = "my-signing-secret"

    def _make_headers(self, body: bytes, ts: float | None = None) -> dict:
        ts_val = str(int(ts or time.time()))
        base = f"v0:{ts_val}:{body.decode()}".encode()
        sig = "v0=" + hmac.new(self.SECRET.encode(), base, hashlib.sha256).hexdigest()
        return {
            "x-slack-signature": sig,
            "x-slack-request-timestamp": ts_val,
        }

    def _make_connector(self) -> SlackConnector:
        from unittest.mock import MagicMock
        connector = SlackConnector()
        connector._secret = self.SECRET
        connector._session = MagicMock()
        return connector

    def test_signature_valide(self):
        connector = self._make_connector()
        body = b'{"type":"event_callback","event":{"type":"message"}}'
        headers = self._make_headers(body)
        assert connector.verify_webhook(body, headers) is True

    def test_signature_invalide(self):
        connector = self._make_connector()
        body = b'{"type":"event_callback"}'
        headers = {
            "x-slack-signature": "v0=invalidsignature",
            "x-slack-request-timestamp": str(int(time.time())),
        }
        assert connector.verify_webhook(body, headers) is False

    def test_timestamp_trop_ancien(self):
        connector = self._make_connector()
        body = b'{"type":"event_callback"}'
        headers = self._make_headers(body, ts=time.time() - 400)
        assert connector.verify_webhook(body, headers) is False

    def test_sans_secret_retourne_true(self):
        connector = SlackConnector()
        connector._secret = None
        connector._session = None
        assert connector.verify_webhook(b"body", {}) is True

    def test_headers_manquants(self):
        connector = self._make_connector()
        assert connector.verify_webhook(b"body", {}) is False


class TestMapMessage:
    def _make_connector(self) -> SlackConnector:
        from unittest.mock import MagicMock
        connector = SlackConnector()
        connector._connected = True
        connector._config = __import__(
            "civitas_acquisition.contracts.models.connector_config",
            fromlist=["ConnectorConfig"]
        ).ConnectorConfig(
            instance_id="inst-slack-1",
            connector_id="slack",
            credentials={"bot_token": "xoxb-test"},
        )
        connector._session = MagicMock()
        return connector

    def test_map_message_simple(self):
        connector = self._make_connector()
        msg = {
            "ts": "1705320000.123456",
            "user": "U01ABCDEF",
            "text": "Hello world! Check this out.",
            "type": "message",
            "reply_count": 0,
        }
        doc = connector._map_message(msg, "C01CHANNEL", "1705320000.123456")
        payload = json.loads(doc.content)

        assert payload["text"] == "Hello world! Check this out."
        assert payload["user"] == "U01ABCDEF"
        assert payload["channel_id"] == "C01CHANNEL"
        assert payload["ts"] == "1705320000.123456"
        assert doc.content_type == "application/json"
        assert "message" in doc.tags
        assert "channel:C01CHANNEL" in doc.tags

    def test_map_message_uri_unique(self):
        connector = self._make_connector()
        msg1 = {"ts": "1705320000.000001", "user": "U1", "text": "msg1", "type": "message", "reply_count": 0}
        msg2 = {"ts": "1705320000.000002", "user": "U1", "text": "msg2", "type": "message", "reply_count": 0}
        doc1 = connector._map_message(msg1, "C1", msg1["ts"])
        doc2 = connector._map_message(msg2, "C1", msg2["ts"])
        assert doc1.id != doc2.id
        assert doc1.source_ref.uri != doc2.source_ref.uri

    def test_map_message_cursor_timestamp(self):
        connector = self._make_connector()
        msg = {"ts": "1705320000.123456", "user": "U1", "text": "msg", "type": "message", "reply_count": 0}
        doc = connector._map_message(msg, "C1", "1705320000.123456")
        assert doc.cursor.value == "1705320000.123456"
        assert doc.cursor.source_type == "timestamp"

    def test_map_message_avec_thread(self):
        connector = self._make_connector()
        msg = {
            "ts": "1705320000.111111",
            "user": "U1",
            "text": "Thread parent",
            "type": "message",
            "thread_ts": "1705320000.111111",
            "reply_count": 3,
        }
        doc = connector._map_message(msg, "C1", msg["ts"])
        payload = json.loads(doc.content)
        assert payload["reply_count"] == 3
        assert payload["reply_count"] == 3
        assert doc.source_metadata["has_thread"] is True

    def test_map_message_source_metadata(self):
        connector = self._make_connector()
        msg = {"ts": "1705320000.999", "user": "U99", "text": "test", "type": "message", "reply_count": 0}
        doc = connector._map_message(msg, "C99", msg["ts"])
        assert doc.source_metadata["resource_type"] == "message"
        assert doc.source_metadata["channel_id"] == "C99"
        assert doc.source_metadata["user"] == "U99"


class TestCursorComposite:
    def _make_connector(self) -> SlackConnector:
        from unittest.mock import MagicMock
        from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
        connector = SlackConnector()
        connector._config = ConnectorConfig(
            instance_id="inst-slack-1", connector_id="slack",
            credentials={"bot_token": "xoxb-test"},
        )
        connector._session = MagicMock()
        return connector

    def test_make_cursor_serialise_json(self):
        connector = self._make_connector()
        cursors = {"msg:C01": "1705320000.000001", "msg:C02": "1705320100.000001"}
        cursor = connector._make_cursor(cursors)
        parsed = json.loads(cursor.value)
        assert parsed["msg:C01"] == "1705320000.000001"
        assert cursor.source_type == "token"
        assert cursor.connector_id == "slack"

    def test_stamp_remplace_cursor(self):
        import dataclasses
        connector = self._make_connector()
        from civitas_acquisition.contracts.models.raw_document import RawDocument
        doc = RawDocument.create(
            instance_id="inst-slack-1", connector_id="slack",
            uri="slack://message/C1/1705320000.000001",
            content=b"content", content_type="application/json",
        )
        cursors = {"msg:C1": "1705320000.999999"}
        stamped = connector._stamp(doc, cursors)
        assert stamped.cursor is not None
        assert "msg:C1" in stamped.cursor.value
