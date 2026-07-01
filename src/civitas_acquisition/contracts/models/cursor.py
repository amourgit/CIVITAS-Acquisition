"""
Cursor — position opaque dans une source externe.

Le curseur est opaque du point de vue du pipeline.
Seul le connecteur sait comment l'interpréter et l'avancer.
Sa sérialisation est JSON-safe (string value) pour la persistence.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

CursorSourceType = Literal[
    "timestamp",   # ISO-8601 datetime string
    "sequence",    # Monotonic integer
    "token",       # Opaque pagination token (GitHub, Notion, ...)
    "etag",        # HTTP ETag for conditional requests
    "offset",      # Kafka/queue byte or message offset
    "page",        # Page number (last resort, less reliable)
]


@dataclass(frozen=True)
class Cursor:
    """
    Opaque checkpoint representing progress within a source.

    Rules:
    - value must be monotonically increasing (or lexicographically comparable)
    - The cursor is advanced ONLY after the document is successfully written
      to the Raw Repository. Never before. This guarantees exactly-once delivery.
    - Stored per connector instance_id in the CursorTracker.
    """

    value: str
    source_type: CursorSourceType
    connector_id: str
    instance_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "value": self.value,
            "source_type": self.source_type,
            "connector_id": self.connector_id,
            "instance_id": self.instance_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> Cursor:
        return cls(
            value=data["value"],
            source_type=data["source_type"],  # type: ignore[arg-type]
            connector_id=data["connector_id"],
            instance_id=data["instance_id"],
        )

    def __str__(self) -> str:
        return f"Cursor({self.source_type}={self.value!r}, instance={self.instance_id!r})"
