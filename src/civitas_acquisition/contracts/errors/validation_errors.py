"""
Validation exceptions — raised by the pipeline's validator stage.
A ValidationError always causes the document to be sent to the DLQ.
It is never retried (the document itself is invalid, not the infrastructure).
"""

from .base import AcquisitionError


class ValidationError(AcquisitionError):
    """Base for all document validation errors. Non-retryable."""


class SchemaValidationError(ValidationError):
    def __init__(self, field: str, reason: str, value: object = None) -> None:
        super().__init__(
            f"Schema validation failed on field '{field}': {reason}",
            context={
                "field": field,
                "value": str(value) if value is not None else None,
            },
        )
        self.field = field


class ContentPolicyError(ValidationError):
    """The document violates a content policy rule."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"Content policy violation: {reason}")


class SizeLimitError(ValidationError):
    def __init__(self, actual_bytes: int, max_bytes: int) -> None:
        super().__init__(
            f"Document size {actual_bytes:,} bytes exceeds limit of {max_bytes:,} bytes",
            context={"actual_bytes": actual_bytes, "max_bytes": max_bytes},
        )
        self.actual_bytes = actual_bytes
        self.max_bytes = max_bytes


class InvalidContentTypeError(ValidationError):
    def __init__(self, content_type: str, allowed: list[str]) -> None:
        super().__init__(
            f"Content type '{content_type}' is not accepted. "
            f"Allowed: {', '.join(allowed)}",
            context={"content_type": content_type, "allowed": allowed},
        )
        self.content_type = content_type


class ChecksumMismatchError(ValidationError):
    def __init__(self, expected: str, actual: str) -> None:
        super().__init__(
            "Checksum mismatch — document may be corrupted",
            context={"expected": expected, "actual": actual},
        )


class EmptyContentError(ValidationError):
    def __init__(self) -> None:
        super().__init__("Document content is empty")


class MissingRequiredFieldError(ValidationError):
    def __init__(self, field: str) -> None:
        super().__init__(
            f"Required field '{field}' is missing or null",
            context={"field": field},
        )
