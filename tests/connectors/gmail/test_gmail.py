"""Tests GmailConnector — sans appel réseau."""
import pytest
import json
import base64

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from unittest.mock import MagicMock
from civitas_acquisition.connectors.communication.gmail.connector import GmailConnector
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def make_connector() -> GmailConnector:
    c = GmailConnector()
    c._connected = True
    c._access_token = "ya29.test"
    c._refresh_token = None
    c._client_id = None
    c._client_secret = None
    c._email_address = "user@example.com"
    c._label_ids = ["INBOX"]
    c._resource_types = ["emails"]
    c._max_results = 500
    c._include_spam = False
    c._include_body = True
    c._body_format = "text"
    c._query = ""
    c._max_attach_size = 5 * 1024 * 1024
    c._session = MagicMock()
    from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
    c._config = ConnectorConfig(
        instance_id="inst-gmail-1", connector_id="gmail",
        credentials={"access_token": "ya29.test"},
    )
    return c


def make_message(msg_id="msg001", subject="Test", from_="a@b.com", body="Hello", internal_date="1705320000000") -> dict:
    return {
        "id": msg_id, "threadId": f"t_{msg_id}", "labelIds": ["INBOX"],
        "snippet": body[:50], "internalDate": internal_date,
        "payload": {
            "mimeType": "multipart/alternative", "body": {},
            "headers": [
                {"name": "Subject", "value": subject},
                {"name": "From",    "value": from_},
                {"name": "To",      "value": "user@example.com"},
                {"name": "Date",    "value": "Mon, 15 Jan 2024 12:00:00 +0000"},
            ],
            "parts": [{"mimeType": "text/plain", "body": {"data": _b64(body), "size": len(body)}, "parts": []}],
        },
    }


class TestManifest:
    def test_connector_id(self):
        assert GmailConnector().manifest().connector_id == "gmail"

    def test_polling_channel(self):
        assert ChannelType.POLLING in GmailConnector().manifest().supported_channels

    def test_access_token_requis(self):
        keys = [c.key for c in GmailConnector().manifest().required_credentials]
        assert "access_token" in keys

    def test_refresh_token_optionnel(self):
        keys = [c.key for c in GmailConnector().manifest().optional_credentials]
        assert "refresh_token" in keys and "client_id" in keys


class TestExtractPayload:
    def test_body_text_simple(self):
        c = make_connector()
        payload = {"mimeType": "text/plain", "body": {"data": _b64("Hello!"), "size": 6}, "parts": []}
        text, html, attachments = c._extract_payload(payload)
        assert text == "Hello!"
        assert html == ""
        assert len(attachments) == 0

    def test_multipart_text_et_html(self):
        c = make_connector()
        payload = {
            "mimeType": "multipart/alternative", "body": {},
            "parts": [
                {"mimeType": "text/plain", "body": {"data": _b64("Plain"), "size": 5}, "parts": []},
                {"mimeType": "text/html",  "body": {"data": _b64("<b>Bold</b>"), "size": 11}, "parts": []},
            ],
        }
        text, html, _ = c._extract_payload(payload)
        assert "Plain" in text
        assert "<b>Bold</b>" in html

    def test_attachment_inclus(self):
        c = make_connector()
        payload = {
            "mimeType": "multipart/mixed", "body": {},
            "parts": [
                {"mimeType": "text/plain", "body": {"data": _b64("Body"), "size": 4}, "parts": []},
                {"mimeType": "application/pdf", "filename": "report.pdf",
                 "body": {"size": 1024, "attachmentId": "att001"}, "parts": []},
            ],
        }
        _, _, attachments = c._extract_payload(payload)
        assert len(attachments) == 1
        assert attachments[0]["name"] == "report.pdf"
        assert attachments[0]["attachment_id"] == "att001"

    def test_attachment_trop_grand_exclu(self):
        c = make_connector()
        c._max_attach_size = 100
        payload = {
            "mimeType": "multipart/mixed", "body": {},
            "parts": [
                {"mimeType": "application/zip", "filename": "huge.zip",
                 "body": {"size": 50_000_000, "attachmentId": "att_big"}, "parts": []},
            ],
        }
        _, _, attachments = c._extract_payload(payload)
        assert len(attachments) == 0


class TestMapMessage:
    def test_champs_basiques(self):
        c = make_connector()
        doc = c._map_message(make_message())
        payload = json.loads(doc.content)
        assert payload["id"] == "msg001"
        assert payload["subject"] == "Test"
        assert payload["from"] == "a@b.com"
        assert "Hello" in payload["body_text"]

    def test_content_type(self):
        c = make_connector()
        assert c._map_message(make_message()).content_type == "application/json"

    def test_tags(self):
        c = make_connector()
        doc = c._map_message(make_message())
        assert "email" in doc.tags
        assert any("thread:" in t for t in doc.tags)

    def test_cursor_internal_date(self):
        c = make_connector()
        doc = c._map_message(make_message(internal_date="1705320000000"))
        assert doc.cursor.value == "1705320000000"
        assert doc.cursor.source_type == "sequence"

    def test_uri_unique(self):
        c = make_connector()
        d1 = c._map_message(make_message("msg1"))
        d2 = c._map_message(make_message("msg2"))
        assert d1.id != d2.id
        assert d1.source_ref.uri != d2.source_ref.uri

    def test_source_metadata_complet(self):
        c = make_connector()
        doc = c._map_message(make_message(msg_id="xyz789", subject="Important meeting"))
        m = doc.source_metadata
        assert m["resource_type"] == "email"
        assert m["msg_id"] == "xyz789"
        assert m["subject"] == "Important meeting"
        assert "INBOX" in m["labels"]

    def test_has_attachments(self):
        c = make_connector()
        msg = make_message()
        msg["payload"]["parts"].append({
            "mimeType": "application/pdf", "filename": "doc.pdf",
            "body": {"size": 2048, "attachmentId": "att1"}, "parts": [],
        })
        doc = c._map_message(msg)
        assert doc.source_metadata["has_attachments"] is True

    def test_body_format_html(self):
        c = make_connector()
        c._body_format = "html"
        msg = make_message(body="Text content")
        msg["payload"]["parts"].append({
            "mimeType": "text/html",
            "body": {"data": _b64("<p>HTML content</p>"), "size": 20}, "parts": [],
        })
        doc = c._map_message(msg)
        payload = json.loads(doc.content)
        assert payload["body_text"] == ""
        assert "<p>HTML content</p>" in payload["body_html"]
