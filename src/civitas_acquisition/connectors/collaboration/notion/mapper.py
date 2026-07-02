"""NotionMapper — Notion objects → RawDocument."""
from __future__ import annotations
import json
from civitas_acquisition.connectors.collaboration.notion.models import (
    NotionPage, NotionDatabase, NotionBlock,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.raw_document import RawDocument


class NotionMapper:
    def __init__(self, instance_id: str, connector_id: str = "notion") -> None:
        self._instance_id = instance_id
        self._connector_id = connector_id

    def map_page(self, page: NotionPage, markdown_content: str) -> RawDocument:
        content = markdown_content.encode("utf-8") if markdown_content else b""
        if not content:
            # Fallback JSON si pas de contenu texte
            content = json.dumps({
                "title": page.title,
                "properties": page.properties,
            }, ensure_ascii=False).encode("utf-8")

        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=page.url,
            content=content,
            content_type="text/markdown",
            version=page.last_edited_time,
            cursor=Cursor(
                value=page.last_edited_time,
                source_type="timestamp",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("page", f"parent:{page.parent_type}"),
            source_metadata={
                "resource_type": "page",
                "notion_id": page.id,
                "title": page.title,
                "parent_type": page.parent_type,
                "parent_id": page.parent_id,
                "created_time": page.created_time,
                "last_edited_time": page.last_edited_time,
                "archived": page.archived,
                "icon": page.icon,
            },
        )

    def map_database(self, db: NotionDatabase, rows: list[NotionPage]) -> RawDocument:
        payload = {
            "id": db.id,
            "title": db.title,
            "schema": db.properties,
            "row_count": len(rows),
            "rows": [
                {"id": r.id, "title": r.title, "last_edited": r.last_edited_time}
                for r in rows
            ],
        }
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=db.url,
            content=content,
            content_type="application/json",
            version=db.last_edited_time,
            cursor=Cursor(
                value=db.last_edited_time,
                source_type="timestamp",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("database",),
            source_metadata={
                "resource_type": "database",
                "notion_id": db.id,
                "title": db.title,
                "last_edited_time": db.last_edited_time,
            },
        )
