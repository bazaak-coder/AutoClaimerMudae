"""Deterministic, script-driven mock adapter for tests.

``MockAdapter`` implements :class:`~autoclaimer.adapters.PlatformAdapter`
without ever connecting to Discord or requiring credentials. Tests control
behaviour per operation:

- ``script(operation, *outcomes)`` queues return values or exception
  instances (recoverable, permanent, cooldown) that are consumed FIFO.
- ``delay_seconds`` simulates latency before every operation.
- ``emit(event)`` injects normalized inbound events to subscribers
  (deterministic event-driven testing; no history polling exists).
- Unscripted calls fall back to deterministic defaults (incrementing ids).
- Every call is recorded in ``calls`` as structured test data, and each
  operation also records its domain results (``sent_messages``,
  ``reactions``, ``interactions``).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .adapters import (
    AccountEventPayload,
    ChannelInfo,
    EventConsumer,
    EventDispatcher,
    InteractionResult,
    SentMessage,
)

_FIRST_ID = 900000000000000000


@dataclass(frozen=True)
class MockCall:
    """One recorded adapter invocation, in call order."""

    sequence: int
    operation: str
    arguments: dict[str, Any]


class MockAdapter:
    """In-memory adapter whose behaviour is fully test-controlled."""

    def __init__(self, *, delay_seconds: float = 0.0, event_queue_size: int = 64) -> None:
        self.delay_seconds = delay_seconds
        self.calls: list[MockCall] = []
        self.sent_messages: list[SentMessage] = []
        self.reactions: list[tuple[int, int, str]] = []
        self.interactions: list[InteractionResult] = []
        self.closed = False
        self._scripts: dict[str, deque[Any]] = {}
        self._channels: dict[int, ChannelInfo] = {}
        self._sequence = 0
        self._next_id = _FIRST_ID
        self._events = EventDispatcher(queue_size=event_queue_size, name="mock")

    # ----- test control -----

    def script(self, operation: str, *outcomes: Any) -> None:
        """Queue outcomes (values or ``BaseException`` instances) for an operation."""
        self._scripts.setdefault(operation, deque()).extend(outcomes)

    def add_channel(self, info: ChannelInfo) -> None:
        """Register a cached channel for ``get_channel``/``validate_channel``."""
        self._channels[info.channel_id] = info

    async def emit(self, event: AccountEventPayload) -> None:
        """Deliver a synthetic normalized event to subscribers."""
        await self._events.dispatch(event)

    def subscribe(
        self, consumer: EventConsumer, *, on_events_lost: Callable[[int], None] | None = None
    ) -> Any:
        """Subscribe to synthetic events; returns an unsubscribe callable.

        ``on_events_lost`` mirrors the dispatcher contract (milestone 5):
        queued-but-undelivered events are reported at close() instead of
        vanishing — the account-fatal/eviction path is observable."""
        return self._events.subscribe(consumer, on_events_lost=on_events_lost)

    async def get_channel(self, channel_id: int) -> ChannelInfo | None:
        """Cache-only lookup mirroring the Discord adapter (recorded, no script)."""
        self._record("get_channel", {"channel_id": channel_id})
        return self._channels.get(channel_id)

    async def validate_channel(self, channel_id: int) -> bool:
        """True when the channel was registered via ``add_channel``."""
        self._record("validate_channel", {"channel_id": channel_id})
        return channel_id in self._channels

    # ----- internals -----

    def _record(self, operation: str, arguments: dict[str, Any]) -> None:
        self._sequence += 1
        self.calls.append(MockCall(self._sequence, operation, arguments))

    def _next_id_value(self) -> int:
        self._next_id += 1
        return self._next_id

    async def _next_outcome(self, operation: str) -> Any:
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        outcomes = self._scripts.get(operation)
        if outcomes:
            outcome = outcomes.popleft()
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return None

    # ----- PlatformAdapter operations -----

    async def send_message(
        self, channel_id: int, content: str, *, on_dispatch: Callable[[], None] | None = None
    ) -> SentMessage:
        self._record("send_message", {"channel_id": channel_id, "content": content})
        outcome = await self._next_outcome("send_message")
        sent = (
            outcome
            if outcome is not None
            else SentMessage(
                channel_id=channel_id, message_id=self._next_id_value(), content=content
            )
        )
        self.sent_messages.append(sent)
        # The wire boundary (no pacing in the mock): a reply from here on
        # genuinely answers THIS message.
        if on_dispatch is not None:
            on_dispatch()
        return sent

    async def add_reaction(
        self,
        channel_id: int,
        message_id: int,
        emoji: str,
        *,
        on_dispatch: Callable[[], None] | None = None,
    ) -> None:
        self._record(
            "add_reaction",
            {"channel_id": channel_id, "message_id": message_id, "emoji": emoji},
        )
        await self._next_outcome("add_reaction")
        self.reactions.append((channel_id, message_id, emoji))
        # The wire boundary (BUG-005, mirroring send_message): traffic
        # from here on genuinely answers THIS reaction.
        if on_dispatch is not None:
            on_dispatch()

    async def press_component(
        self,
        channel_id: int,
        message_id: int,
        component_id: str,
        *,
        on_dispatch: Callable[[], None] | None = None,
    ) -> InteractionResult:
        self._record(
            "press_component",
            {"channel_id": channel_id, "message_id": message_id, "component_id": component_id},
        )
        outcome = await self._next_outcome("press_component")
        result = (
            outcome
            if outcome is not None
            else InteractionResult(
                message_id=message_id, component_id=component_id, acknowledged=True
            )
        )
        self.interactions.append(result)
        # Wire boundary (mirrors add_reaction): traffic from here on
        # genuinely answers THIS press.
        if on_dispatch is not None:
            on_dispatch()
        return result

    async def close(self) -> None:
        self._record("close", {})
        await self._events.stop()
        self.closed = True
