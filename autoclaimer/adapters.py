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

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class AdapterError(Exception):
    """Base class for adapter operation failures."""


class RecoverableAdapterError(AdapterError):
    """Transient failure; the same operation may succeed on retry."""


class PermanentAdapterError(AdapterError):
    """Non-retryable failure (bad request, forbidden, missing target, ...)."""


class UncertainOutcomeError(AdapterError):
    """The platform may have applied the action but the response was lost.

    Never auto-retried: the framework records the uncertainty instead of
    blindly repeating a possibly-applied mutation (PR #1 finding 3).
    """


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
class InteractionResult:
    """Outcome of pressing a message component (for example a button)."""

    message_id: int
    component_id: str
    acknowledged: bool


@dataclass(frozen=True)
class ChannelMessage:
    """A message observed on the platform."""

    id: int
    channel_id: int
    author_id: int
    content: str
    has_components: bool = False
    created_at: datetime | None = None


# ----- normalized inbound events -----


@dataclass(frozen=True)
class AccountEvent:
    """Base class for events delivered from one account's client."""

    account_id: int
    received_at: datetime  # timezone-aware UTC


@dataclass(frozen=True)
class MessageCreatedEvent(AccountEvent):
    """A new message was received."""

    message: ChannelMessage


@dataclass(frozen=True)
class ReactionAddedEvent(AccountEvent):
    """A reaction was added to a (cached) message."""

    channel_id: int
    message_id: int
    user_id: int
    emoji: str
    #: Cached message context; absent when the platform does not provide it.
    message: ChannelMessage | None = None


@dataclass(frozen=True)
class MessageEditedEvent(AccountEvent):
    """A message was edited (delivered via the platform's raw edit event).

    ``before`` is the cached pre-edit snapshot when available; uncached
    edits still deliver with ``before=None``.
    """

    before: ChannelMessage | None
    after: ChannelMessage


@dataclass(frozen=True)
class RawReactionAddedEvent(AccountEvent):
    """A reaction was added (raw event: no cache requirement).

    Fires for EVERY reaction, including reactions to uncached messages,
    carrying only identifiers and the emoji. When the message IS cached
    the adapter additionally emits :class:`ReactionAddedEvent` afterwards
    (platform dispatch order: raw first, rich second) with full message
    context — consumers should subscribe to one or the other, not both,
    unless double delivery is desired.
    """

    channel_id: int
    message_id: int
    user_id: int
    emoji: str
    #: Author of the reacted-to message when the platform provides it.
    message_author_id: int | None = None


AccountEventPayload = (
    MessageCreatedEvent | ReactionAddedEvent | MessageEditedEvent | RawReactionAddedEvent
)
#: Consumers may be sync (e.g. ``list.append``) or async callables.
EventConsumer = Callable[[AccountEventPayload], Awaitable[None] | None]


class EventDispatcher:
    """Fan-out inbound events to subscribers with isolation and bounds.

    Each subscriber gets its own bounded queue and one pump task: a slow
    or failing consumer never blocks or breaks the others. When a queue is
    full the OLDEST event is dropped (fresh state wins) and the drop is
    counted and logged. Per-subscriber delivery is FIFO in the order the
    adapter dispatched the events.
    """

    def __init__(self, *, queue_size: int = 64, name: str = "events") -> None:
        self._queue_size = queue_size
        self._name = name
        self._subscribers: dict[int, _Subscription] = {}
        self._next_id = 0
        self.dropped = 0

    def subscribe(self, consumer: EventConsumer) -> Callable[[], None]:
        """Register *consumer*; returns an unsubscribe callable."""
        self._next_id += 1
        handle = self._next_id
        name = getattr(consumer, "__name__", f"consumer-{handle}")
        subscription = _Subscription(consumer, asyncio.Queue(self._queue_size), name=name)
        subscription.pump = asyncio.get_running_loop().create_task(
            self._pump(subscription), name=f"{self._name}-pump-{handle}"
        )
        self._subscribers[handle] = subscription

        def unsubscribe() -> None:
            removed = self._subscribers.pop(handle, None)
            if removed is not None and removed.pump is not None:
                removed.pump.cancel()

        return unsubscribe

    async def dispatch(self, event: AccountEventPayload) -> None:
        """Deliver *event* to every subscriber (drop-oldest on overflow)."""
        for subscription in list(self._subscribers.values()):
            queue = subscription.queue
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:  # pragma: no cover - raced with pump
                    pass
                self.dropped += 1
                subscription.dropped += 1
                logger.warning(
                    "event queue full; dropped oldest event",
                    extra={
                        "account_id": event.account_id,
                        "operation": f"{self._name}.dispatch",
                        "outcome": "dropped",
                        "scope": subscription.name,
                    },
                )
            queue.put_nowait(event)

    async def _pump(self, subscription: _Subscription) -> None:
        while True:
            event = await subscription.queue.get()
            try:
                result = subscription.consumer(event)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "event consumer failed; continuing",
                    extra={
                        "account_id": event.account_id,
                        "operation": f"{self._name}.consumer",
                        "outcome": "consumer_error",
                    },
                )

    async def stop(self, *, pump_timeout: float = 5.0) -> None:
        """Cancel and await all pump tasks (shutdown).

        Bounded: a consumer that suppresses cancellation cannot hang
        shutdown forever (survivors are logged and abandoned). Safe to
        call from inside a consumer (the calling pump is skipped).
        """
        current = asyncio.current_task()
        pumps = [
            s.pump
            for s in self._subscribers.values()
            if s.pump is not None and s.pump is not current
        ]
        self._subscribers.clear()
        for pump in pumps:
            pump.cancel()
        for pump in pumps:
            try:
                await asyncio.wait_for(asyncio.shield(pump), timeout=pump_timeout)
            except (asyncio.CancelledError, TimeoutError):
                pass
            except Exception:
                logger.exception("event pump failed to stop cleanly")


@dataclass
class _Subscription:
    consumer: EventConsumer
    queue: asyncio.Queue[AccountEventPayload]
    name: str = "consumer"
    dropped: int = 0
    pump: asyncio.Task[None] | None = None


@runtime_checkable
class PlatformAdapter(Protocol):
    """Minimum asynchronous operations the framework needs from a platform."""

    async def send_message(self, channel_id: int, content: str) -> SentMessage:
        """Send *content* to *channel_id* and return the sent message."""
        ...

    async def add_reaction(self, channel_id: int, message_id: int, emoji: str) -> None:
        """React to a message with *emoji*."""
        ...

    async def press_component(
        self, channel_id: int, message_id: int, component_id: str
    ) -> InteractionResult:
        """Press a component (button) attached to a message."""
        ...

    def subscribe(self, consumer: EventConsumer) -> Callable[[], None]:
        """Subscribe to normalized inbound events; returns unsubscribe."""
        ...

    async def close(self) -> None:
        """Release adapter resources. Must be safe to call more than once."""
        ...
