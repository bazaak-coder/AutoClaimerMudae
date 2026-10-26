"""Platform-independent adapter contracts.

The framework observes a chat platform and acts on it exclusively through
:class:`PlatformAdapter`, an asynchronous interface that knows nothing about
any concrete client. Concrete adapters (a future ``discord.py-self``
implementation, or the in-package mock) own the client lifecycle,
authentication, and all platform-specific details.

Operation results and expected errors are modeled explicitly so callers can
distinguish transient from permanent failures and server-directed cooldowns
without parsing platform payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


class AdapterError(Exception):
    """Base class for adapter operation failures."""


class RecoverableAdapterError(AdapterError):
    """Transient failure; the same operation may succeed on retry."""


class PermanentAdapterError(AdapterError):
    """Non-retryable failure (bad request, forbidden, missing target, ...)."""


class CooldownError(RecoverableAdapterError):
    """Server-directed cooldown; the operation must not be retried before
    ``retry_after_seconds`` has elapsed."""

    def __init__(self, retry_after_seconds: float, message: str | None = None) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            message or f"Server-directed cooldown: retry after {retry_after_seconds:.3f}s."
        )


@dataclass(frozen=True)
class SentMessage:
    """Confirmation that a message was sent."""

    channel_id: int
    message_id: int
    content: str


@dataclass(frozen=True)
class ChannelMessage:
    """A message observed in a channel."""

    id: int
    channel_id: int
    author_id: int
    content: str
    has_components: bool = False


@dataclass(frozen=True)
class InteractionResult:
    """Outcome of pressing a message component (for example a button)."""

    message_id: int
    component_id: str
    acknowledged: bool


@runtime_checkable
class PlatformAdapter(Protocol):
    """Minimum asynchronous operations the framework needs from a platform."""

    async def send_message(self, channel_id: int, content: str) -> SentMessage:
        """Send *content* to *channel_id* and return the sent message."""
        ...

    async def fetch_recent_messages(
        self, channel_id: int, limit: int = 50
    ) -> tuple[ChannelMessage, ...]:
        """Return up to *limit* recent messages, most recent last."""
        ...

    async def add_reaction(self, channel_id: int, message_id: int, emoji: str) -> None:
        """React to a message with *emoji*."""
        ...

    async def press_component(
        self, channel_id: int, message_id: int, component_id: str
    ) -> InteractionResult:
        """Press a component (button) attached to a message."""
        ...

    async def close(self) -> None:
        """Release adapter resources. Must be safe to call more than once."""
        ...
