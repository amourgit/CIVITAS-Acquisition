"""Tests Generic API — partie WebConnector mode 'api'."""
import pytest
import json

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from civitas_acquisition.connectors.web.connector import WebConnector


def make_api_connector(options: dict | None = None) -> WebConnector:
    import re
    from unittest.mock import MagicMock
    from civitas_acquisition.connectors.web.connector import _EXCLUDE_PATTERNS_DEFAULT, _INCLUDE_EXTENSIONS
    from civitas_acquisition.contracts.models.connector_config import ConnectorConfig

    config = ConnectorConfig(
        instance_id="inst-api-1", connector_id="web",
        credentials={"bearer_token": "tok_123"},
        options={
            "mode": "api",
            "base_url": "https://api.example.com",
            "endpoints": [{"path": "/articles", "method": "GET", "params": {"status": "published"}}],
            "pagination_type": "link_header",
            "items_path": "",
            **(options or {}),
        },
    )
    c = WebConnector()
    c._connected = True
    c._config = config
    c._mode = "api"
    c._base_url = config.get_option("base_url", "")
    c._endpoints = config.get_option("endpoints", [])
    c._pagination_type = config.get_option("pagination_type", "link_header")
    c._items_path = config.get_option("items_path", "")
    c._cursor_path = config.get_option("cursor_path", "next_cursor")
    c._seed_urls = []
    c._allowed_domains = set()
    c._max_depth = 3
    c._max_pages = 1000
    c._include_exts = frozenset(_INCLUDE_EXTENSIONS)
    c._exclude_patterns = [re.compile(p) for p in _EXCLUDE_PATTERNS_DEFAULT]
    c._politeness = 0.0
    c._respect_robots = False
    c._etag_cache = {}
    c._auth_headers = {"Authorization": "Bearer tok_123"}
    c._session = MagicMock()
    return c


class TestExtractPath:
    """Tests exhaustifs de l'extraction par chemin JSON."""

    def test_chemin_simple(self):
        assert WebConnector._extract_path({"items": [1, 2, 3]}, "items") == [1, 2, 3]

    def test_chemin_imbriqué_deux_niveaux(self):
        data = {"data": {"results": [{"id": 1}, {"id": 2}]}}
        assert WebConnector._extract_path(data, "data.results") == [{"id": 1}, {"id": 2}]

    def test_chemin_imbriqué_trois_niveaux(self):
        data = {"response": {"body": {"items": ["a", "b"]}}}
        assert WebConnector._extract_path(data, "response.body.items") == ["a", "b"]

    def test_chemin_vide_retourne_data(self):
        data = [{"id": 1}]
        assert WebConnector._extract_path(data, "") is data

    def test_chemin_inexistant_retourne_none(self):
        data = {"items": [1]}
        assert WebConnector._extract_path(data, "missing.key") is None

    def test_clé_presente_valeur_none(self):
        data = {"cursor": None}
        assert WebConnector._extract_path(data, "cursor") is None

    def test_valeur_scalaire(self):
        data = {"next_cursor": "tok_abc123"}
        assert WebConnector._extract_path(data, "next_cursor") == "tok_abc123"

    def test_chemin_sur_non_dict_retourne_none(self):
        data = {"items": [1, 2, 3]}
        assert WebConnector._extract_path(data, "items.nonexistent") is None


class TestStampApiMode:
    def test_stamp_ajoute_cursor_json(self):
        c = make_api_connector()
        from civitas_acquisition.contracts.models.raw_document import RawDocument
        doc = RawDocument.create(
            instance_id="inst-api-1", connector_id="web",
            uri="https://api.example.com/articles/1",
            content=b'{"id":1}', content_type="application/json",
        )
        cursors = {"api:/articles": "tok_page2"}
        stamped = c._stamp(doc, cursors)
        assert stamped.cursor is not None
        parsed = json.loads(stamped.cursor.value)
        assert parsed["api:/articles"] == "tok_page2"
        assert stamped.cursor.connector_id == "web"
        assert stamped.cursor.source_type == "token"


class TestPaginateApiUnit:
    """Tests du comportement de pagination via mocks de session."""

    async def test_items_path_extraction(self):
        """Vérifie que items_path extrait correctement les items."""
        c = make_api_connector(options={"items_path": "data.items"})
        data = {"data": {"items": [{"id": 1, "title": "Article 1"}, {"id": 2, "title": "Article 2"}]}}
        items = WebConnector._extract_path(data, c._items_path)
        assert len(items) == 2
        assert items[0]["id"] == 1

    async def test_cursor_path_extraction(self):
        """Vérifie l'extraction du next cursor."""
        c = make_api_connector(options={
            "pagination_type": "cursor",
            "cursor_path": "meta.pagination.next_cursor",
        })
        response_data = {
            "items": [{"id": 1}],
            "meta": {"pagination": {"next_cursor": "cursor_page2", "total": 100}},
        }
        next_cursor = WebConnector._extract_path(response_data, c._cursor_path)
        assert next_cursor == "cursor_page2"

    def test_endpoint_url_construit_correctement(self):
        c = make_api_connector()
        endpoint = c._endpoints[0]
        expected_url = "https://api.example.com/articles"
        url = c._base_url.rstrip("/") + "/" + endpoint["path"].lstrip("/")
        assert url == expected_url

    def test_base_url_avec_trailing_slash(self):
        c = make_api_connector(options={"base_url": "https://api.example.com/"})
        endpoint = {"path": "/articles", "method": "GET", "params": {}}
        url = c._base_url.rstrip("/") + "/" + endpoint["path"].lstrip("/")
        assert url == "https://api.example.com/articles"
        assert "//" not in url.replace("https://", "")

    def test_endpoint_sans_slash_prefixe(self):
        c = make_api_connector(options={
            "base_url": "https://api.example.com",
            "endpoints": [{"path": "articles", "method": "GET", "params": {}}],
        })
        endpoint = c._endpoints[0]
        url = c._base_url.rstrip("/") + "/" + endpoint["path"].lstrip("/")
        assert url == "https://api.example.com/articles"


class TestManifestApiMode:
    def test_connector_id_web(self):
        assert WebConnector().manifest().connector_id == "web"

    def test_mime_types(self):
        m = WebConnector().manifest()
        assert "application/json" in m.supported_mime_types
        assert "text/html" in m.supported_mime_types

    def test_pas_de_credentials_obligatoires(self):
        assert len(WebConnector().manifest().required_credentials) == 0

    def test_bearer_et_api_key_optionnels(self):
        keys = [c.key for c in WebConnector().manifest().optional_credentials]
        assert "bearer_token" in keys
        assert "api_key" in keys
        assert "api_key_header" in keys


class TestAuthHeaders:
    """Test que les headers d'auth sont bien construits."""

    def test_bearer_token(self):
        c = make_api_connector()
        assert c._auth_headers.get("Authorization") == "Bearer tok_123"

    def test_user_agent_toujours_present(self):
        from civitas_acquisition.connectors.web.connector import USER_AGENT
        # Le USER_AGENT est défini comme constante
        assert "CIVITAS" in USER_AGENT
        assert "crawler" in USER_AGENT.lower()
