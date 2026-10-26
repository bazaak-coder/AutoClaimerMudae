"""Deterministic, script-driven mock adapter for tests.

``MockAdapter`` implements :class:`~autoclaimer.adapters.PlatformAdapter`
without ever connecting to Discord or requiring credentials. Tests control
behaviour per operation:

- ``script(operation, *outcomes)`` queues return values or exception
  instances (recoverable, permanent, cooldown) that are consumed FIFO.
- ``delay_seconds`` simulates latency before every operation.
- ``set_channel_messages`` seeds observable channel state.
- Unscripted calls fall back to deterministic defaults (incrementing ids).
- Every call is recorded in ``calls`` as structured test data, and each
  operation also records its domain results (``sent_messages``,
  ``reactions``, ``interactions``).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .adapters import ChannelMessage, InteractionResult, SentMessage

_FIRST_ID = 900000000000000000


@dataclass(frozen=True)
class MockCall:
    """One recorded adapter invocation, in call order."""

    sequence: int
    operation: str
    arguments: dict[str, Any]


class MockAdapter:
    """In-memory adapter whose behaviour is fully test-controlled."""

    def __init__(self, *, delay_seconds: float = 0.0) -> None:
        self.delay_seconds = delay_seconds
        self.calls: list[MockCall] = []
        self.sent_messages: list[SentMessage] = []
        self.reactions: list[tuple[int, int, str]] = []
        self.interactions: list[InteractionResult] = []
        self.channel_messages: dict[int, tuple[ChannelMessage, ...]] = {}
        self.closed = False
        self._scripts: dict[str, deque[Any]] = {}
        self._sequence = 0
        self._next_id = _FIRST_ID

    # ----- test control -----

    def script(self, operation: str, *outcomes: Any) -> None:
        """Queue outcomes (values or ``BaseException`` instances) for an operation."""
        self._scripts.setdefault(operation, deque()).extend(outcomes)

    def set_channel_messages(self, channel_id: int, messages: Iterable[ChannelMessage]) -> None:
        """Seed the messages observable via ``fetch_recent_messages``."""
        self.channel_messages[channel_id] = tuple(messages)

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

    async def send_message(self, channel_id: int, content: str) -> SentMessage:
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
        return sent

    async def fetch_recent_messages(
        self, channel_id: int, limit: int = 50
    ) -> tuple[ChannelMessage, ...]:
        self._record("fetch_recent_messages", {"channel_id": channel_id, "limit": limit})
        outcome = await self._next_outcome("fetch_recent_messages")
        if outcome is not None:
            return tuple(outcome)
        messages = self.channel_messages.get(channel_id, ())
        return messages[-limit:] if limit > 0 else ()

    async def add_reaction(self, channel_id: int, message_id: int, emoji: str) -> None:
        self._record(
            "add_reaction",
            {"channel_id": channel_id, "message_id": message_id, "emoji": emoji},
        )
        await self._next_outcome("add_reaction")
        self.reactions.append((channel_id, message_id, emoji))

    async def press_component(
        self, channel_id: int, message_id: int, component_id: str
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
        return result

    async def close(self) -> None:
        self._record("close", {})
        self.closed = True
