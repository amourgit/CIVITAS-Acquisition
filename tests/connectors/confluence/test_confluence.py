"""Tests ConfluenceConnector — sans appel réseau."""
import pytest
import json

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

pytestmark = pytest.mark.skipif(not HAS_AIOHTTP, reason="aiohttp not installed")

from unittest.mock import MagicMock
from civitas_acquisition.connectors.collaboration.confluence.connector import (
    ConfluenceConnector, _strip_html, _storage_to_markdown,
)
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import ChannelType


def make_connector(format_: str = "text") -> ConfluenceConnector:
    c = ConfluenceConnector()
    c._connected = True
    c._base_url = "https://company.atlassian.net/wiki"
    c._space_keys = []
    c._resource_types = ["pages"]
    c._max_pages = 10_000
    c._include_archived = False
    c._content_format = format_
    c._expand = "body.storage,version,ancestors"
    c._session = MagicMock()
    from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
    c._config = ConnectorConfig(
        instance_id="inst-conf-1", connector_id="confluence",
        credentials={"base_url": "https://company.atlassian.net/wiki", "email": "e@mail.com", "api_token": "tok"},
    )
    return c


def make_page(
    page_id="12345", title="Architecture Guide",
    body_html="<h1>Intro</h1><p>This is the content.</p>",
    space_key="ARCH", version=3, modified="2024-01-15T12:00:00.000Z",
) -> dict:
    return {
        "id": page_id, "type": "page", "title": title,
        "version": {"number": version, "when": modified},
        "space": {"key": space_key, "name": "Architecture"},
        "ancestors": [{"title": "Parent Page"}],
        "_links": {"webui": f"/display/{space_key}/{page_id}"},
        "body": {"storage": {"value": body_html}},
    }


class TestManifest:
    def test_connector_id(self):
        assert ConfluenceConnector().manifest().connector_id == "confluence"

    def test_channels(self):
        m = ConfluenceConnector().manifest()
        assert ChannelType.POLLING in m.supported_channels
        assert ChannelType.WEBHOOK in m.supported_channels

    def test_credentials_requis(self):
        keys = [c.key for c in ConfluenceConnector().manifest().required_credentials]
        assert "base_url" in keys
        assert "email" in keys
        assert "api_token" in keys


class TestStripHtml:
    def test_html_simple(self):
        assert "Hello world" in _strip_html("<p>Hello world</p>")

    def test_tags_imbriques(self):
        result = _strip_html("<div><h1>Title</h1><p>Content <strong>bold</strong></p></div>")
        assert "Title" in result
        assert "Content" in result
        assert "bold" in result

    def test_html_vide(self):
        assert _strip_html("") == ""

    def test_espaces_multiples(self):
        result = _strip_html("<p>   text   </p>")
        assert "text" in result


class TestStorageToMarkdown:
    def test_heading_h1(self):
        md = _storage_to_markdown("<h1>Architecture</h1>")
        assert md.startswith("# Architecture")

    def test_heading_h2(self):
        md = _storage_to_markdown("<h2>Section</h2>")
        assert "## Section" in md

    def test_bold(self):
        md = _storage_to_markdown("<strong>Important</strong>")
        assert "**Important**" in md

    def test_italic(self):
        md = _storage_to_markdown("<em>Note</em>")
        assert "*Note*" in md

    def test_code_inline(self):
        md = _storage_to_markdown("<code>print('hello')</code>")
        assert "`print('hello')`" in md

    def test_paragraphe(self):
        md = _storage_to_markdown("<p>First paragraph</p>")
        assert "First paragraph" in md

    def test_liste(self):
        md = _storage_to_markdown("<ul><li>Item one</li><li>Item two</li></ul>")
        assert "- Item one" in md
        assert "- Item two" in md


class TestMapContent:
    def test_format_text(self):
        c = make_connector(format_="text")
        page = make_page(title="My Guide", body_html="<h1>Intro</h1><p>Content here.</p>")
        doc = c._map_content(page)
        assert doc is not None
        assert "My Guide" in doc.content.decode()
        assert doc.content_type == "text/plain"

    def test_format_markdown(self):
        c = make_connector(format_="markdown")
        page = make_page(title="Guide", body_html="<h1>Title</h1><p>Body text.</p>")
        doc = c._map_content(page)
        assert doc is not None
        assert doc.content_type == "text/markdown"
        assert "# Guide" in doc.content.decode()

    def test_none_si_storage_et_title_vides(self):
        c = make_connector()
        page = make_page(title="", body_html="   ")
        doc = c._map_content(page)
        assert doc is None

    def test_source_metadata_complet(self):
        c = make_connector()
        page = make_page(page_id="99", title="Deploy Guide", space_key="DEV", version=5)
        doc = c._map_content(page)
        assert doc is not None
        m = doc.source_metadata
        assert m["content_id"] == "99"
        assert m["title"] == "Deploy Guide"
        assert m["space_key"] == "DEV"
        assert m["version"] == 5
        assert m["resource_type"] == "page"

    def test_tags_contiennent_space_et_type(self):
        c = make_connector()
        doc = c._map_content(make_page(space_key="PROD"))
        assert "page" in doc.tags
        assert "space:PROD" in doc.tags

    def test_cursor_last_modified(self):
        c = make_connector()
        doc = c._map_content(make_page(modified="2024-01-20T09:00:00.000Z"))
        assert doc.cursor.value == "2024-01-20T09:00:00.000Z"
        assert doc.cursor.source_type == "timestamp"

    def test_ancestors_dans_metadata(self):
        c = make_connector()
        page = make_page()
        doc = c._map_content(page)
        assert "Parent Page" in doc.source_metadata["ancestors"]

    def test_uri_basee_sur_base_url(self):
        c = make_connector()
        doc = c._map_content(make_page(space_key="ARCH", page_id="12345"))
        assert "company.atlassian.net/wiki" in doc.source_ref.uri

    def test_version_dans_source_ref(self):
        c = make_connector()
        doc = c._map_content(make_page(version=7))
        assert doc.source_ref.version == "7"

    def test_blogpost_type(self):
        c = make_connector()
        page = make_page()
        page["type"] = "blogpost"
        doc = c._map_content(page)
        assert "blogpost" in doc.tags


class TestMapSpace:
    def test_map_space_basique(self):
        c = make_connector()
        space = {
            "key": "ARCH",
            "name": "Architecture Team",
            "description": {"plain": {"value": "All architecture docs"}},
        }
        doc = c._map_space(space)
        payload = json.loads(doc.content)
        assert payload["key"] == "ARCH"
        assert payload["name"] == "Architecture Team"
        assert "space" in doc.tags
        assert doc.source_metadata["resource_type"] == "space"
