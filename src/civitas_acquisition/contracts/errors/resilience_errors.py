"""
Resilience layer exceptions.
RetryEngine, CircuitBreaker, Vault, and DLQ failures.
"""

from .base import AcquisitionError


class ResilienceError(AcquisitionError):
    """Base for all resilience errors."""


# ── Retry ─────────────────────────────────────────────────────────────────────

class MaxRetriesExhaustedError(ResilienceError):
    def __init__(self, attempts: int, last_error: str = "") -> None:
        super().__init__(
            f"Max retries exhausted after {attempts} attempt(s)"
            + (f": {last_error}" if last_error else ""),
            context={"attempts": attempts},
        )
        self.attempts = attempts


# ── Circuit Breaker ───────────────────────────────────────────────────────────

class CircuitOpenError(ResilienceError):
    """
    The circuit breaker is OPEN for this resource.
    All calls are blocked until the recovery timeout expires.
    """

    def __init__(self, resource_id: str) -> None:
        super().__init__(
            f"Circuit breaker is OPEN for '{resource_id}'. Calls are blocked.",
            context={"resource_id": resource_id},
        )
        self.resource_id = resource_id


# ── Vault ─────────────────────────────────────────────────────────────────────

class VaultError(ResilienceError):
    """Base for vault errors."""


class VaultSecretNotFoundError(VaultError):
    def __init__(self, path: str) -> None:
        super().__init__(
            f"Secret not found at path '{path}'",
            context={"path": path},
        )
        self.path = path


class VaultAccessDeniedError(VaultError):
    def __init__(self, path: str) -> None:
        super().__init__(
            f"Access denied to secret at path '{path}'",
            context={"path": path},
        )


class VaultConnectionError(VaultError):
    def __init__(self, reason: str = "") -> None:
        super().__init__(
            f"Vault connection failed: {reason}" if reason
            else "Vault connection failed",
        )


class SecretExpiredError(VaultError):
    def __init__(self, path: str) -> None:
        super().__init__(
            f"Secret at '{path}' has expired and must be rotated.",
            context={"path": path},
        )


# ── DLQ ──────────────────────────────────────────────────────────────────────

class DLQError(ResilienceError):
    """Base for dead letter queue errors."""


class DLQWriteError(DLQError):
    def __init__(self, document_id: str, reason: str) -> None:
        super().__init__(
            f"Failed to write document '{document_id}' to DLQ: {reason}",
            context={"document_id": document_id},
        )


class DLQReplayError(DLQError):
    def __init__(self, document_id: str, reason: str) -> None:
        super().__init__(
            f"Failed to replay document '{document_id}' from DLQ: {reason}",
            context={"document_id": document_id},
        )
