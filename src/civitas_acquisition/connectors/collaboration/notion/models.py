"""Notion-specific value objects — internes au connecteur."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class NotionPage:
    id: str               # UUID Notion (avec tirets)
    url: str
    title: str
    parent_type: str      # "workspace", "page_id", "database_id"
    parent_id: Optional[str]
    created_time: str     # ISO-8601
    last_edited_time: str
    created_by: str       # user ID
    last_edited_by: str
    archived: bool
    properties: dict[str, Any] = field(default_factory=dict)
    icon: Optional[str] = None
    cover: Optional[str] = None

    @property
    def notion_id(self) -> str:
        return self.id.replace("-", "")

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> NotionPage:
        parent = data.get("parent", {})
        parent_type = parent.get("type", "workspace")
        parent_id = parent.get(parent_type) if parent_type != "workspace" else None
        title = cls._extract_title(data.get("properties", {}))
        return cls(
            id=data["id"],
            url=data.get("url", ""),
            title=title,
            parent_type=parent_type,
            parent_id=parent_id,
            created_time=data.get("created_time", ""),
            last_edited_time=data.get("last_edited_time", ""),
            created_by=data.get("created_by", {}).get("id", ""),
            last_edited_by=data.get("last_edited_by", {}).get("id", ""),
            archived=data.get("archived", False),
            properties=data.get("properties", {}),
            icon=cls._extract_icon(data.get("icon")),
            cover=data.get("cover", {}).get("external", {}).get("url") if data.get("cover") else None,
        )

    @staticmethod
    def _extract_title(properties: dict[str, Any]) -> str:
        for key in ("title", "Name", "Title"):
            if key in properties:
                prop = properties[key]
                if prop.get("type") == "title":
                    parts = prop.get("title", [])
                    return "".join(p.get("plain_text", "") for p in parts)
        return "Untitled"

    @staticmethod
    def _extract_icon(icon: Optional[dict]) -> Optional[str]:
        if not icon:
            return None
        if icon.get("type") == "emoji":
            return icon.get("emoji")
        if icon.get("type") == "external":
            return icon.get("external", {}).get("url")
        return None


@dataclass(frozen=True)
class NotionDatabase:
    id: str
    url: str
    title: str
    created_time: str
    last_edited_time: str
    properties: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> NotionDatabase:
        title_parts = data.get("title", [])
        title = "".join(p.get("plain_text", "") for p in title_parts) or "Untitled DB"
        return cls(
            id=data["id"],
            url=data.get("url", ""),
            title=title,
            created_time=data.get("created_time", ""),
            last_edited_time=data.get("last_edited_time", ""),
            properties={k: v.get("type") for k, v in data.get("properties", {}).items()},
        )


@dataclass(frozen=True)
class NotionBlock:
    id: str
    type: str
    content: str          # Texte extrait du bloc
    children: list[NotionBlock] = field(default_factory=list)

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> NotionBlock:
        block_type = data.get("type", "")
        block_data = data.get(block_type, {})
        content = cls._extract_text(block_data)
        return cls(id=data["id"], type=block_type, content=content)

    @staticmethod
    def _extract_text(block_data: dict[str, Any]) -> str:
        rich_texts = block_data.get("rich_text", [])
        return "".join(rt.get("plain_text", "") for rt in rich_texts)
