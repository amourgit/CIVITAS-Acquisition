"""
CIVITAS Acquisition — Error contracts.
Import from here, never from submodules directly.
"""

from .base import AcquisitionError
from .connector_errors import (
    ConnectorError,
    ConnectorNotFoundError,
    ConnectorAlreadyRegisteredError,
    ManifestValidationError,
    ConnectorNotConnectedError,
    ConnectorAlreadyConnectedError,
    ConnectorAuthenticationError,
    ConnectorNetworkError,
    ConnectorRateLimitError,
    ConnectorTemporaryError,
    ConnectorFatalError,
    ConnectorTimeoutError,
)
from .channel_errors import (
    ChannelError,
    ChannelNotStartedError,
    ChannelAlreadyRunningError,
    WebhookSignatureInvalidError,
    WebhookTimestampExpiredError,
    WebhookReplayAttackError,
    WebhookParseError,
    StreamingOffsetError,
    StreamingConsumerGroupError,
    QueueConnectionError,
    MessageAcknowledgmentError,
)
from .resilience_errors import (
    ResilienceError,
    MaxRetriesExhaustedError,
    CircuitOpenError,
    VaultError,
    VaultSecretNotFoundError,
    VaultAccessDeniedError,
    VaultConnectionError,
    SecretExpiredError,
    DLQError,
    DLQWriteError,
    DLQReplayError,
)
from .validation_errors import (
    ValidationError,
    SchemaValidationError,
    ContentPolicyError,
    SizeLimitError,
    InvalidContentTypeError,
    ChecksumMismatchError,
    EmptyContentError,
    MissingRequiredFieldError,
)

__all__ = [
    "AcquisitionError",
    "ConnectorError", "ConnectorNotFoundError", "ConnectorAlreadyRegisteredError",
    "ManifestValidationError", "ConnectorNotConnectedError", "ConnectorAlreadyConnectedError",
    "ConnectorAuthenticationError", "ConnectorNetworkError", "ConnectorRateLimitError",
    "ConnectorTemporaryError", "ConnectorFatalError", "ConnectorTimeoutError",
    "ChannelError", "ChannelNotStartedError", "ChannelAlreadyRunningError",
    "WebhookSignatureInvalidError", "WebhookTimestampExpiredError", "WebhookReplayAttackError",
    "WebhookParseError", "StreamingOffsetError", "StreamingConsumerGroupError",
    "QueueConnectionError", "MessageAcknowledgmentError",
    "ResilienceError", "MaxRetriesExhaustedError", "CircuitOpenError",
    "VaultError", "VaultSecretNotFoundError", "VaultAccessDeniedError",
    "VaultConnectionError", "SecretExpiredError",
    "DLQError", "DLQWriteError", "DLQReplayError",
    "ValidationError", "SchemaValidationError", "ContentPolicyError",
    "SizeLimitError", "InvalidContentTypeError", "ChecksumMismatchError",
    "EmptyContentError", "MissingRequiredFieldError",
]
