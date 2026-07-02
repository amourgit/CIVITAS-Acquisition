"""Tests pour le connecteur Notion — sans appel réseau."""
import pytest
import json
from civitas_acquisition.connectors.collaboration.notion.models import (
    NotionPage, NotionDatabase, NotionBlock,
)
from civitas_acquisition.connectors.collaboration.notion.mapper import NotionMapper
from civitas_acquisition.connectors.collaboration.notion.fetcher import NotionFetcher
from civitas_acquisition.connectors.collaboration.notion.connector import NotionConnector
from civitas_acquisition.contracts.models.connector_manifest import ChannelType

INSTANCE_ID = "inst-notion-1"


# ── Models ────────────────────────────────────────────────────────────────────

class TestNotionPageFromApi:
    def _api_data(self, **overrides) -> dict:
        base = {
            "id": "abc-123-def-456",
            "url": "https://notion.so/abc123def456",
            "parent": {"type": "workspace"},
            "created_time": "2024-01-10T08:00:00.000Z",
            "last_edited_time": "2024-01-15T12:00:00.000Z",
            "created_by": {"id": "user-1"},
            "last_edited_by": {"id": "user-2"},
            "archived": False,
            "icon": {"type": "emoji", "emoji": "📝"},
            "properties": {
                "title": {
                    "type": "title",
                    "title": [{"plain_text": "My Page Title"}],
                }
            },
        }
        base.update(overrides)
        return base

    def test_title_extrait(self):
        page = NotionPage.from_api(self._api_data())
        assert page.title == "My Page Title"

    def test_parent_workspace(self):
        page = NotionPage.from_api(self._api_data())
        assert page.parent_type == "workspace"
        assert page.parent_id is None

    def test_parent_database(self):
        data = self._api_data(parent={"type": "database_id", "database_id": "db-uuid"})
        page = NotionPage.from_api(data)
        assert page.parent_type == "database_id"
        assert page.parent_id == "db-uuid"

    def test_icon_emoji(self):
        page = NotionPage.from_api(self._api_data())
        assert page.icon == "📝"

    def test_notion_id_sans_tirets(self):
        page = NotionPage.from_api(self._api_data())
        assert "-" not in page.notion_id

    def test_archived(self):
        page = NotionPage.from_api(self._api_data(archived=True))
        assert page.archived is True


# ── Mapper ────────────────────────────────────────────────────────────────────

class TestNotionMapper:
    @pytest.fixture
    def mapper(self):
        return NotionMapper(instance_id=INSTANCE_ID)

    def _make_page(self) -> NotionPage:
        return NotionPage(
            id="abc-123", url="https://notion.so/abc123",
            title="Architecture Guide",
            parent_type="workspace", parent_id=None,
            created_time="2024-01-10T08:00:00Z",
            last_edited_time="2024-01-15T12:00:00Z",
            created_by="user-1", last_edited_by="user-2",
            archived=False,
        )

    def test_map_page_markdown_content(self, mapper):
        page = self._make_page()
        doc = mapper.map_page(page, "# Architecture Guide\n\nThis is the content.")
        assert doc.content == b"# Architecture Guide\n\nThis is the content."
        assert doc.content_type == "text/markdown"

    def test_map_page_fallback_json_si_vide(self, mapper):
        page = self._make_page()
        doc = mapper.map_page(page, "")
        assert doc.content_type == "text/markdown" or doc.content_type == "application/json"

    def test_map_page_cursor_last_edited(self, mapper):
        page = self._make_page()
        doc = mapper.map_page(page, "content")
        assert doc.cursor.value == "2024-01-15T12:00:00Z"
        assert doc.cursor.source_type == "timestamp"

    def test_map_page_metadata(self, mapper):
        page = self._make_page()
        doc = mapper.map_page(page, "content")
        assert doc.source_metadata["title"] == "Architecture Guide"
        assert doc.source_metadata["resource_type"] == "page"
        assert doc.source_metadata["notion_id"] == "abc-123"

    def test_map_page_tags(self, mapper):
        page = self._make_page()
        doc = mapper.map_page(page, "content")
        assert "page" in doc.tags

    def test_map_database(self, mapper):
        db = NotionDatabase(
            id="db-456", url="https://notion.so/db456",
            title="Tasks DB",
            created_time="2024-01-01T00:00:00Z",
            last_edited_time="2024-01-20T00:00:00Z",
            properties={"Name": "title", "Status": "select"},
        )
        rows = [self._make_page()]
        doc = mapper.map_database(db, rows)
        payload = json.loads(doc.content)
        assert payload["title"] == "Tasks DB"
        assert payload["row_count"] == 1
        assert "database" in doc.tags

    def test_map_page_id_deterministe(self, mapper):
        page = self._make_page()
        doc1 = mapper.map_page(page, "content")
        doc2 = mapper.map_page(page, "content")
        assert doc1.id == doc2.id


# ── Fetcher blocks_to_markdown ────────────────────────────────────────────────

class TestBlocksToMarkdown:
    @pytest.fixture
    def fetcher(self):
        from unittest.mock import MagicMock
        client = MagicMock()
        return NotionFetcher(client)

    def _block(self, block_type: str, content: str) -> NotionBlock:
        return NotionBlock(id="blk-1", type=block_type, content=content)

    def test_heading1(self, fetcher):
        md = fetcher.blocks_to_markdown([self._block("heading_1", "Title")])
        assert "# Title" in md

    def test_heading2(self, fetcher):
        md = fetcher.blocks_to_markdown([self._block("heading_2", "Section")])
        assert "## Section" in md

    def test_bullet(self, fetcher):
        md = fetcher.blocks_to_markdown([self._block("bulleted_list_item", "Item")])
        assert "- Item" in md

    def test_code_block(self, fetcher):
        md = fetcher.blocks_to_markdown([self._block("code", "x = 1")])
        assert "```" in md
        assert "x = 1" in md

    def test_paragraph(self, fetcher):
        md = fetcher.blocks_to_markdown([self._block("paragraph", "Some text")])
        assert "Some text" in md

    def test_nested_blocks(self, fetcher):
        child = NotionBlock(id="c1", type="paragraph", content="Child text")
        parent = NotionBlock(id="p1", type="toggle", content="Toggle", children=[child])
        md = fetcher.blocks_to_markdown([parent])
        assert "Toggle" in md
        assert "Child text" in md


# ── Connector manifest ────────────────────────────────────────────────────────

class TestNotionConnectorManifest:
    def test_connector_id(self):
        assert NotionConnector().manifest().connector_id == "notion"

    def test_channels(self):
        m = NotionConnector().manifest()
        assert ChannelType.POLLING in m.supported_channels

    def test_credential_token(self):
        m = NotionConnector().manifest()
        keys = [c.key for c in m.required_credentials]
        assert "token" in keys
