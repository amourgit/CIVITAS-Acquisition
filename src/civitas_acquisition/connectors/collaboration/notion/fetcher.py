"""NotionFetcher — récupère pages, databases, blocs et contenu."""
from __future__ import annotations
import logging
from typing import AsyncIterator, Optional
from civitas_acquisition.connectors.collaboration.notion.client import NotionClient, ResourceNotFoundError
from civitas_acquisition.connectors.collaboration.notion.models import (
    NotionPage, NotionDatabase, NotionBlock,
)

logger = logging.getLogger(__name__)


class NotionFetcher:
    def __init__(self, client: NotionClient) -> None:
        self._client = client

    # ── Search / Discovery ────────────────────────────────────────────────────

    async def search_pages(
        self, since: Optional[str] = None,
    ) -> AsyncIterator[NotionPage]:
        """Recherche toutes les pages accessibles par l'intégration."""
        body: dict = {"filter": {"value": "page", "property": "object"}, "sort": {"direction": "ascending", "timestamp": "last_edited_time"}}
        if since:
            # Notion ne supporte pas `since` directement en search
            # On filtre côté client sur last_edited_time
            pass
        async for page_batch in self._client.paginate("/search", body=body):
            for item in page_batch:
                page = NotionPage.from_api(item)
                if since and page.last_edited_time <= since:
                    continue
                yield page

    async def search_databases(
        self, since: Optional[str] = None,
    ) -> AsyncIterator[NotionDatabase]:
        """Recherche toutes les databases accessibles."""
        body: dict = {"filter": {"value": "database", "property": "object"}}
        async for batch in self._client.paginate("/search", body=body):
            for item in batch:
                db = NotionDatabase.from_api(item)
                if since and db.last_edited_time <= since:
                    continue
                yield db

    async def query_database(
        self, database_id: str, since: Optional[str] = None,
    ) -> AsyncIterator[NotionPage]:
        """Récupère tous les items d'une database."""
        body: dict = {"sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}]}
        if since:
            body["filter"] = {
                "timestamp": "last_edited_time",
                "last_edited_time": {"after": since},
            }
        path = f"/databases/{database_id}/query"
        async for batch in self._client.paginate(path, body=body):
            for item in batch:
                yield NotionPage.from_api(item)

    # ── Page content ──────────────────────────────────────────────────────────

    async def fetch_page_content(self, page_id: str) -> list[NotionBlock]:
        """Récupère tous les blocs d'une page (récursif)."""
        return await self._fetch_blocks_recursive(page_id, depth=0)

    async def _fetch_blocks_recursive(
        self, block_id: str, depth: int, max_depth: int = 5,
    ) -> list[NotionBlock]:
        if depth > max_depth:
            return []
        blocks: list[NotionBlock] = []
        try:
            async for batch in self._client.get_children(block_id):
                for item in batch:
                    block = NotionBlock.from_api(item)
                    if item.get("has_children", False) and depth < max_depth:
                        children = await self._fetch_blocks_recursive(
                            item["id"], depth=depth + 1, max_depth=max_depth
                        )
                        block = NotionBlock(
                            id=block.id, type=block.type,
                            content=block.content, children=children,
                        )
                    blocks.append(block)
        except ResourceNotFoundError:
            logger.debug("Block %s not found", block_id)
        return blocks

    def blocks_to_markdown(self, blocks: list[NotionBlock], indent: int = 0) -> str:
        """Convertit les blocs Notion en Markdown."""
        lines: list[str] = []
        prefix = "  " * indent
        for block in blocks:
            text = block.content
            match block.type:
                case "heading_1": lines.append(f"# {text}")
                case "heading_2": lines.append(f"## {text}")
                case "heading_3": lines.append(f"### {text}")
                case "bulleted_list_item": lines.append(f"{prefix}- {text}")
                case "numbered_list_item": lines.append(f"{prefix}1. {text}")
                case "to_do": lines.append(f"{prefix}- [ ] {text}")
                case "toggle": lines.append(f"{prefix}> {text}")
                case "code": lines.append(f"```\n{text}\n```")
                case "quote": lines.append(f"> {text}")
                case "divider": lines.append("---")
                case "paragraph": lines.append(text) if text else None
                case _: lines.append(text) if text else None
            if block.children:
                lines.append(self.blocks_to_markdown(block.children, indent + 1))
        return "\n\n".join(l for l in lines if l)
