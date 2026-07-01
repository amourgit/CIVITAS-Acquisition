"""
Connector-specific exceptions.
Classified into temporary (retryable) vs fatal (non-retryable) errors.
The RetryEngine uses this classification to decide whether to retry.
"""

from __future__ import annotations
from .base import AcquisitionError


class ConnectorError(AcquisitionError):
    """Base for all connector errors."""


# ── Discovery & Registry ──────────────────────────────────────────────────────

class ConnectorNotFoundError(ConnectorError):
    def __init__(self, connector_id: str, available: list[str] | None = None) -> None:
        super().__init__(
            f"Connector '{connector_id}' not found in registry",
            context={"connector_id": connector_id, "available": available or []},
        )
        self.connector_id = connector_id
        self.available = available or []


class ConnectorAlreadyRegisteredError(ConnectorError):
    def __init__(self, connector_id: str) -> None:
        super().__init__(
            f"Connector '{connector_id}' is already registered",
            context={"connector_id": connector_id},
        )


class ManifestValidationError(ConnectorError):
    def __init__(self, connector_id: str, field: str, reason: str) -> None:
        super().__init__(
            f"Invalid manifest for '{connector_id}': field '{field}' — {reason}",
            context={"connector_id": connector_id, "field": field},
        )


# ── Lifecycle ─────────────────────────────────────────────────────────────────

class ConnectorNotConnectedError(ConnectorError):
    def __init__(self, connector_id: str) -> None:
        super().__init__(
            f"Connector '{connector_id}' is not connected. Call connect() first.",
            context={"connector_id": connector_id},
        )


class ConnectorAlreadyConnectedError(ConnectorError):
    def __init__(self, connector_id: str) -> None:
        super().__init__(
            f"Connector '{connector_id}' is already connected.",
            context={"connector_id": connector_id},
        )


# ── Authentication ────────────────────────────────────────────────────────────

class ConnectorAuthenticationError(ConnectorError):
    """
    Fatal. Do not retry — credentials are invalid or expired.
    Operator action required: refresh credentials in vault.
    """

    def __init__(self, connector_id: str, reason: str = "") -> None:
        super().__init__(
            f"Authentication failed for connector '{connector_id}': {reason}",
            context={"connector_id": connector_id},
        )


# ── Network & Availability ────────────────────────────────────────────────────

class ConnectorNetworkError(ConnectorError):
    """Temporary. Retryable."""

    def __init__(self, connector_id: str, url: str = "", cause: str = "") -> None:
        super().__init__(
            f"Network error for connector '{connector_id}'"
            + (f" at {url}" if url else "")
            + (f": {cause}" if cause else ""),
            context={"connector_id": connector_id, "url": url},
        )


class ConnectorRateLimitError(ConnectorError):
    """
    Temporary. Retryable after retry_after_s seconds.
    The RetryEngine should respect retry_after_s instead of its default backoff.
    """

    def __init__(
        self, connector_id: str, retry_after_s: float | None = None
    ) -> None:
        super().__init__(
            f"Rate limit exceeded for connector '{connector_id}'"
            + (f". Retry after {retry_after_s}s" if retry_after_s else ""),
            context={"connector_id": connector_id, "retry_after_s": retry_after_s},
        )
        self.retry_after_s = retry_after_s


class ConnectorTemporaryError(ConnectorError):
    """Generic temporary error. Retryable."""


class ConnectorFatalError(ConnectorError):
    """Generic fatal error. Do not retry."""


class ConnectorTimeoutError(ConnectorError):
    """Request timed out. Temporary. Retryable."""

    def __init__(self, connector_id: str, timeout_s: float) -> None:
        super().__init__(
            f"Connector '{connector_id}' timed out after {timeout_s}s",
            context={"connector_id": connector_id, "timeout_s": timeout_s},
        )
