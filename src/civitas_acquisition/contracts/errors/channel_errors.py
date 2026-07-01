"""
Channel-specific exceptions.
Covers inbound webhook security, streaming offset errors, and queue failures.
"""

from .base import AcquisitionError


class ChannelError(AcquisitionError):
    """Base for all channel errors."""


class ChannelNotStartedError(ChannelError):
    def __init__(self, channel_type: str) -> None:
        super().__init__(
            f"Channel '{channel_type}' is not started. Call start() first.",
            context={"channel_type": channel_type},
        )


class ChannelAlreadyRunningError(ChannelError):
    def __init__(self, channel_type: str) -> None:
        super().__init__(
            f"Channel '{channel_type}' is already running.",
            context={"channel_type": channel_type},
        )


# ── Webhook ───────────────────────────────────────────────────────────────────

class WebhookSignatureInvalidError(ChannelError):
    """
    The webhook payload signature does not match.
    Always reject — do not process the payload.
    """

    def __init__(self, reason: str = "") -> None:
        super().__init__(
            f"Webhook signature is invalid: {reason}" if reason
            else "Webhook signature is invalid",
        )


class WebhookTimestampExpiredError(ChannelError):
    """
    The webhook timestamp is outside the acceptable window.
    Protects against replay attacks.
    """

    def __init__(self, age_s: float, max_age_s: float) -> None:
        super().__init__(
            f"Webhook timestamp is too old: {age_s:.1f}s > max {max_age_s:.1f}s",
            context={"age_s": age_s, "max_age_s": max_age_s},
        )


class WebhookReplayAttackError(ChannelError):
    """
    This event ID was already processed.
    The delivery is a duplicate — silently discard.
    """

    def __init__(self, event_id: str) -> None:
        super().__init__(
            f"Replay attack detected: event '{event_id}' was already processed.",
            context={"event_id": event_id},
        )


class WebhookParseError(ChannelError):
    """Could not parse the webhook payload."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"Failed to parse webhook payload: {reason}")


# ── Streaming ─────────────────────────────────────────────────────────────────

class StreamingOffsetError(ChannelError):
    def __init__(self, partition: int, offset: int, reason: str = "") -> None:
        super().__init__(
            f"Streaming offset error at partition={partition}, offset={offset}"
            + (f": {reason}" if reason else ""),
            context={"partition": partition, "offset": offset},
        )


class StreamingConsumerGroupError(ChannelError):
    def __init__(self, group_id: str, reason: str) -> None:
        super().__init__(
            f"Consumer group '{group_id}' error: {reason}",
            context={"group_id": group_id},
        )


# ── Queue ─────────────────────────────────────────────────────────────────────

class QueueConnectionError(ChannelError):
    def __init__(self, queue_url: str, reason: str = "") -> None:
        super().__init__(
            f"Failed to connect to queue '{queue_url}': {reason}",
            context={"queue_url": queue_url},
        )


class MessageAcknowledgmentError(ChannelError):
    def __init__(self, message_id: str, reason: str) -> None:
        super().__init__(
            f"Failed to acknowledge message '{message_id}': {reason}",
            context={"message_id": message_id},
        )
