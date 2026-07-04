"""Tests WebConnector — crawler + Generic API, sans appel réseau."""
import pytest
import json

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from unittest.mock import MagicMock
from civitas_acquisition.connectors.web.connector import WebConnector
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType


def make_connector(options: dict | None = None) -> WebConnector:
    config = ConnectorConfig(
        instance_id="inst-web-1", connector_id="web",
        credentials={},
        options={
            "seed_urls": ["https://docs.example.com"],
            "allowed_domains": ["docs.example.com"],
            "max_depth": 2,
            **(options or {}),
        },
    )
    c = WebConnector()
    c._connected = True
    c._config = config
    import re
    from civitas_acquisition.connectors.web.connector import _EXCLUDE_PATTERNS_DEFAULT, _INCLUDE_EXTENSIONS
    c._seed_urls = config.get_option("seed_urls", [])
    c._allowed_domains = set(config.get_option("allowed_domains", []))
    c._max_depth = config.get_option("max_depth", 3)
    c._max_pages = config.get_option("max_pages", 1000)
    c._include_exts = frozenset(config.get_option("include_extensions", list(_INCLUDE_EXTENSIONS)))
    c._exclude_patterns = [re.compile(p) for p in _EXCLUDE_PATTERNS_DEFAULT]
    c._base_url = config.get_option("base_url", "")
    c._endpoints = config.get_option("endpoints", [])
    c._pagination_type = config.get_option("pagination_type", "link_header")
    c._items_path = config.get_option("items_path", "")
    c._cursor_path = config.get_option("cursor_path", "next_cursor")
    c._politeness = 0.0  # pas de délai dans les tests
    c._respect_robots = False
    c._etag_cache = {}
    c._mode = config.get_option("mode", "crawler")
    c._session = MagicMock()
    return c


class TestManifest:
    def test_connector_id(self):
        assert WebConnector().manifest().connector_id == "web"

    def test_canaux(self):
        m = WebConnector().manifest()
        assert ChannelType.POLLING in m.supported_channels
        assert ChannelType.MANUAL in m.supported_channels

    def test_pas_de_credentials_requis(self):
        m = WebConnector().manifest()
        assert len(m.required_credentials) == 0

    def test_optional_auth(self):
        keys = [c.key for c in WebConnector().manifest().optional_credentials]
        assert "bearer_token" in keys
        assert "api_key" in keys
        assert "basic_username" in keys


class TestExtractLinks:
    def test_liens_absolus_extraits(self):
        c = make_connector()
        html = '<a href="https://docs.example.com/guide">Guide</a><a href="https://docs.example.com/api">API</a>'
        links = c._extract_links(html, "https://docs.example.com")
        assert "https://docs.example.com/guide" in links
        assert "https://docs.example.com/api" in links

    def test_liens_relatifs_resolus(self):
        c = make_connector()
        html = '<a href="/docs/intro">Intro</a><a href="../reference">Ref</a>'
        links = c._extract_links(html, "https://docs.example.com/guide/")
        assert any("intro" in l for l in links)

    def test_domaines_externes_exclus(self):
        c = make_connector()
        html = '<a href="https://evil.com/steal">Evil</a><a href="https://docs.example.com/ok">OK</a>'
        links = c._extract_links(html, "https://docs.example.com")
        assert not any("evil.com" in l for l in links)
        assert any("docs.example.com" in l for l in links)

    def test_mailto_exclus(self):
        c = make_connector()
        html = '<a href="mailto:admin@example.com">Email</a>'
        links = c._extract_links(html, "https://docs.example.com")
        assert len(links) == 0

    def test_javascript_exclus(self):
        c = make_connector()
        html = '<a href="javascript:void(0)">Click</a>'
        links = c._extract_links(html, "https://docs.example.com")
        assert len(links) == 0

    def test_duplicates_dedupliques(self):
        c = make_connector()
        html = '<a href="https://docs.example.com/page">P</a><a href="https://docs.example.com/page">P</a>'
        links = c._extract_links(html, "https://docs.example.com")
        assert links.count("https://docs.example.com/page") == 1


class TestIsAllowed:
    def test_url_autorisee(self):
        c = make_connector()
        assert c._is_allowed("https://docs.example.com/guide") is True

    def test_domaine_externe_interdit(self):
        c = make_connector()
        assert c._is_allowed("https://evil.com/page") is False

    def test_pattern_exclusion_images(self):
        c = make_connector()
        assert c._is_allowed("https://docs.example.com/logo.png") is False
        assert c._is_allowed("https://docs.example.com/photo.jpg") is False

    def test_pattern_exclusion_auth(self):
        c = make_connector()
        assert c._is_allowed("https://docs.example.com/login") is False
        assert c._is_allowed("https://docs.example.com/signup") is False

    def test_utm_exclus(self):
        c = make_connector()
        assert c._is_allowed("https://docs.example.com/page?utm_source=email") is False

    def test_page_normale_autorisee(self):
        c = make_connector()
        assert c._is_allowed("https://docs.example.com/architecture/overview") is True


class TestExtractPath:
    def test_chemin_simple(self):
        data = {"items": [1, 2, 3]}
        assert WebConnector._extract_path(data, "items") == [1, 2, 3]

    def test_chemin_imbriqué(self):
        data = {"data": {"results": [{"id": 1}]}}
        assert WebConnector._extract_path(data, "data.results") == [{"id": 1}]

    def test_chemin_vide(self):
        data = [{"id": 1}]
        assert WebConnector._extract_path(data, "") == data

    def test_chemin_inexistant(self):
        data = {"items": [1, 2, 3]}
        assert WebConnector._extract_path(data, "nonexistent.deep") is None


class TestStamp:
    def test_stamp_ajoute_cursor(self):
        c = make_connector()
        from civitas_acquisition.contracts.models.raw_document import RawDocument
        doc = RawDocument.create(
            instance_id="inst-web-1", connector_id="web",
            uri="https://docs.example.com/page",
            content=b"<html>content</html>", content_type="text/html",
        )
        cursors = {"__visited": "https://docs.example.com/page"}
        stamped = c._stamp(doc, cursors)
        assert stamped.cursor is not None
        assert "__visited" in stamped.cursor.value
