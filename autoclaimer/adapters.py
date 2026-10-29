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
from collections.abc import Awaitable, Callable, Mapping
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


class AccountFatalError(PermanentAdapterError):
    """The account itself is dead, not just one operation.

    Raised when the platform rejects the account's credentials
    (``LoginFailure``/``AuthFailure``), invalidates the gateway session
    for an unrecoverable reason (fatal close codes such as 4004), or
    blocks the account (CAPTCHA challenge). Unlike a plain
    :class:`PermanentAdapterError` — which fails only the current task —
    the worker manager must **stop the account**: fail every queued
    instance, disable the account, and shut its worker down. Never
    retried; re-enabling the account is the explicit owner-controlled
    revive path (owner-directed policy, 2026-10-28).
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
class ChannelInfo:
    """A channel the account can see (gateway-cache view, no API call).

    ``channel_type`` is the platform's type name (for Discord:
    ``text``, ``private`` for DMs, ``group``, ``news``, threads, ...).
    Deliberately minimal — no permission lists or member data, which
    would require heavier endpoints.
    """

    channel_id: int
    name: str
    channel_type: str


@dataclass(frozen=True)
class InteractionResult:
    """Outcome of pressing a message component (for example a button)."""

    message_id: int
    component_id: str
    acknowledged: bool


@dataclass(frozen=True)
class EmbedText:
    """Text fields of one embed, carried through inbound events.

    Mudae speaks in embeds (character cards: author=character,
    description=stats/kakera, footer=markers); the adapter preserves
    the per-field text so the parser's source-targeted rules
    (embed_author/embed_title/embed_description/embed_footer) keep
    matching exactly as they do on the raw library objects. No embed
    objects or metadata cross the adapter boundary.
    """

    author_name: str | None = None
    title: str | None = None
    description: str | None = None
    footer: str | None = None


@dataclass(frozen=True)
class ChannelMessage:
    """A message observed on the platform."""

    id: int
    channel_id: int
    author_id: int
    content: str
    has_components: bool = False
    created_at: datetime | None = None
    embeds: tuple[EmbedText, ...] = ()


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
    or failing consumer never blocks or breaks the others. When a queue
    is full the OLDEST event is dropped (fresh state wins) and the drop
    is counted and logged. Per-subscriber delivery is FIFO in the order
    the adapter dispatched the events.

    Event-loss recovery (milestone 5): subscribers that must not miss
    actionable events register ``on_events_lost`` — invoked with the
    subscriber's CUMULATIVE dropped-event count whenever overflow drops
    events AND when ``stop()`` abandons that subscriber's still-queued
    events (adapter eviction, shutdown). Consumers diff the cumulative
    count and reconcile (e.g. re-check Mudae cooldowns) instead of
    assuming every event was processed. ``stats()`` exposes live queue
    pressure and drop counters.
    """

    def __init__(self, *, queue_size: int = 64, name: str = "events") -> None:
        if queue_size <= 0:
            # Audit (leak-F3): a non-positive size yields an UNBOUNDED
            # queue on this platform (full() never True) — the bounded
            # contract would silently vanish. Reject loudly.
            raise ValueError("queue_size must be a positive integer")
        self._queue_size = queue_size
        self._name = name
        self._subscribers: dict[int, _Subscription] = {}
        self._next_id = 0
        self.dropped = 0
        self._stopped = False

    def subscribe(
        self,
        consumer: EventConsumer,
        *,
        on_events_lost: Callable[[int], None] | None = None,
    ) -> Callable[[], None]:
        """Register *consumer*; returns an unsubscribe callable.

        ``on_events_lost`` (optional) receives the subscriber's
        CUMULATIVE lost-event count each time events are lost for it —
        a deliberate isolation boundary: callback exceptions are logged
        and never break dispatch (agents.md section 17.4).

        Raises ``RuntimeError`` after ``stop()``: stop is terminal and a
        post-stop subscription would silently drop every dispatch (its
        pump exits immediately), so it is rejected loudly instead.
        """
        if self._stopped:
            raise RuntimeError(
                "EventDispatcher is stopped; subscriptions are no longer "
                "accepted (stop is terminal — create a new dispatcher)"
            )
        self._next_id += 1
        handle = self._next_id
        name = getattr(consumer, "__name__", f"consumer-{handle}")
        subscription = _Subscription(
            consumer, asyncio.Queue(self._queue_size), name=name, on_loss=on_events_lost
        )
        subscription.pump = asyncio.get_running_loop().create_task(
            self._pump(subscription), name=f"{self._name}-pump-{handle}"
        )
        self._subscribers[handle] = subscription

        def unsubscribe() -> None:
            # Consolidated review #3: unsubscribing is an abandoning
            # teardown — undelivered work is accounted and reported, not
            # silently discarded. AFTER stop() began, stop() owns the
            # terminal accounting (audit F1): a loss callback firing
            # during stop may unsubscribe synchronously — then this is
            # pure removal + pump cancellation, never a second count.
            removed = self._subscribers.pop(handle, None)
            if removed is None:
                return
            removed.stopped = True  # the pump exits after its current callback
            if self._stopped:
                if removed.pump is not None and removed.pump is not asyncio.current_task():
                    removed.pump.cancel()
                return
            current = asyncio.current_task()
            if removed.pump is not None and removed.pump is current:
                # Self-unsubscribe: the current callback finishes (its
                # event is NOT a loss); only queued events are abandoned.
                queued = removed.queue.qsize()
                if queued > 0:
                    self.dropped += queued
                    removed.dropped += queued
                    logger.warning(
                        "subscription unsubscribed with queued events; reported lost",
                        extra={
                            "operation": f"{self._name}.unsubscribe",
                            "outcome": "lost",
                            "scope": removed.name,
                        },
                    )
                    self._notify_loss(removed)
                return
            undelivered = removed.queue.qsize() + (1 if removed.in_flight else 0)
            if undelivered > 0:
                self.dropped += undelivered
                removed.dropped += undelivered
                logger.warning(
                    "subscription unsubscribed with undelivered events; reported lost",
                    extra={
                        "operation": f"{self._name}.unsubscribe",
                        "outcome": "lost",
                        "scope": removed.name,
                    },
                )
                self._notify_loss(removed)
            if removed.pump is not None:
                removed.pump.cancel()

        return unsubscribe

    async def dispatch(self, event: AccountEventPayload) -> None:
        """Deliver *event* to every subscriber (drop-oldest on overflow).

        After ``stop()`` the event cannot be delivered to anyone: it is
        counted and logged as lost instead of silently vanishing
        (symmetric with the loud post-stop subscribe rejection).
        """
        if self._stopped:
            self.dropped += 1
            logger.warning(
                "dispatch after stop; event counted as lost",
                extra={
                    "account_id": event.account_id,
                    "operation": f"{self._name}.dispatch",
                    "outcome": "lost_after_stop",
                },
            )
            return
        for handle, subscription in list(self._subscribers.items()):
            if self._subscribers.get(handle) is not subscription or subscription.stopped:
                # Removed mid-dispatch (e.g. an earlier subscriber's loss
                # callback unsubscribed it): never strand the event on a
                # dead queue — count it (audit race-F3/leak-F2).
                self.dropped += 1
                logger.warning(
                    "dispatch to removed subscription; event counted as lost",
                    extra={
                        "account_id": event.account_id,
                        "operation": f"{self._name}.dispatch",
                        "outcome": "lost_dead_subscription",
                        "scope": subscription.name,
                    },
                )
                continue
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
                self._notify_loss(subscription)
                if subscription.stopped or self._stopped:
                    # The loss callback unsubscribed this subscriber (or
                    # stop() began): the event has no live target.
                    self.dropped += 1
                    logger.warning(
                        "subscription removed during dispatch; event counted as lost",
                        extra={
                            "account_id": event.account_id,
                            "operation": f"{self._name}.dispatch",
                            "outcome": "lost_dead_subscription",
                            "scope": subscription.name,
                        },
                    )
                    continue
            queue.put_nowait(event)

    def _notify_loss(self, subscription: _Subscription) -> None:
        """Report cumulative losses to the subscriber's callback, if any.

        Isolation boundary: a broken callback is logged with its
        traceback and never propagates into dispatch/stop.
        """
        if subscription.on_loss is None:
            return
        try:
            subscription.on_loss(subscription.dropped)
        except Exception:
            logger.exception(
                "event-loss callback failed; loss still counted",
                extra={
                    "operation": f"{self._name}.loss_callback",
                    "outcome": "callback_error",
                    "scope": subscription.name,
                },
            )

    def stats(self) -> EventDispatcherStats:
        """Read-only snapshot: subscriber count, lost-event totals, and
        live queue pressure per subscription (keyed by subscribe handle)."""
        return EventDispatcherStats(
            subscriber_count=len(self._subscribers),
            total_dropped=self.dropped,
            subscriber_dropped={
                handle: subscription.dropped for handle, subscription in self._subscribers.items()
            },
            queue_depths={
                handle: subscription.queue.qsize()
                for handle, subscription in self._subscribers.items()
            },
        )

    async def _pump(self, subscription: _Subscription) -> None:
        while not self._stopped and not subscription.stopped:
            event = await subscription.queue.get()
            # In-flight tracking (PR #3 review fix): an event dequeued
            # into its consumer is invisible to queue.qsize(); stop()
            # counts it via `in_flight` so interrupted deliveries are
            # reported as losses instead of vanishing.
            subscription.in_flight = event
            try:
                result = subscription.consumer(event)
                if inspect.isawaitable(result):
                    await result
                subscription.in_flight = None  # delivered
            except asyncio.CancelledError:
                # Preserve in_flight: stop() reports the interrupted
                # delivery as a loss (never swallowed, §17.4.5).
                raise
            except Exception:
                subscription.in_flight = None
                if self._stopped:
                    # Audit leak-F5: stop() already accounted abandoned
                    # work — a late failure of an abandoned consumer must
                    # not double-count it (log only).
                    logger.exception(
                        "abandoned event consumer failed after stop",
                        extra={
                            "operation": f"{self._name}.consumer",
                            "outcome": "consumer_error_after_stop",
                        },
                    )
                    return
                # Consolidated review #2: the consumer FAILED to process
                # this event — that is an unprocessed event for
                # reconciliation purposes: counted and reported as lost
                # (the pump itself continues with later events).
                subscription.dropped += 1
                self.dropped += 1
                logger.exception(
                    "event consumer failed; event counted as lost",
                    extra={
                        "account_id": event.account_id,
                        "operation": f"{self._name}.consumer",
                        "outcome": "consumer_error_lost",
                    },
                )
                self._notify_loss(subscription)

    async def stop(self, *, pump_timeout: float = 5.0) -> None:
        """Cancel and await all pump tasks (shutdown).

        Bounded: a consumer that suppresses cancellation cannot hang
        shutdown forever (survivors are logged and abandoned). Safe to
        call from inside a consumer (the calling pump is skipped).

        Events still queued for a subscriber at stop time are LOST
        (milestone 5): they are added to the loss counters and reported
        through that subscriber's ``on_events_lost`` callback instead of
        vanishing silently (adapter eviction / shutdown accounting).
        """
        current = asyncio.current_task()
        # Terminal stop signal FIRST (third-round review fix): the
        # self-stopping pump may finish its CURRENT consumer call and
        # must then EXIT — never resume draining its queue (those queued
        # events are counted and reported as lost just below, exactly
        # once). Ordinary pumps are cancelled regardless of the signal.
        self._stopped = True
        # Report undelivered events BEFORE tearing pumps down. In-flight
        # events (dequeued by a pump, blocked inside a consumer) are
        # disjoint from the queues — counted together exactly once
        # (PR #3 review fix). EXCEPT the calling pump (follow-up review
        # fix): stop() is safe to call from inside a consumer, that pump
        # is deliberately skipped below and RESUMES after stop()
        # returns — its in-flight event is allowed to complete and is
        # therefore not a loss. Its queued events stay counted: the
        # normal self-stop caller is shutting down, and whether the
        # surviving pump drains them afterwards is not observable here
        # (conservative, documented). Cancellation-suppressing consumers
        # that survive the timeout may still deliver their in-flight
        # event; they are already 'abandoned' per the bounded-stop
        # contract, so their loss report stands.
        calling = next((s for s in self._subscribers.values() if s.pump is current), None)
        # Snapshot iteration (audit F1, both agents): loss callbacks are
        # arbitrary user code that may unsubscribe synchronously — a
        # live-dict iteration would raise mid-teardown and abandon pumps.
        for subscription in list(self._subscribers.values()):
            in_flight_lost = 1 if subscription.in_flight and subscription is not calling else 0
            undelivered = subscription.queue.qsize() + in_flight_lost
            if undelivered > 0:
                self.dropped += undelivered
                subscription.dropped += undelivered
                logger.warning(
                    "event queue abandoned at stop; events reported lost",
                    extra={
                        "operation": f"{self._name}.stop",
                        "outcome": "lost",
                        "scope": subscription.name,
                    },
                )
                self._notify_loss(subscription)
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
            except asyncio.CancelledError:
                pass
            except TimeoutError:
                # Audit leak-F4: the docstring promises survivors are
                # logged — make the abandoned pump observable by name.
                logger.warning(
                    "event pump survived stop timeout; abandoned",
                    extra={
                        "operation": f"{self._name}.stop",
                        "outcome": "pump_abandoned",
                        "scope": pump.get_name(),
                    },
                )
            except Exception:
                logger.exception("event pump failed to stop cleanly")


@dataclass
class _Subscription:
    consumer: EventConsumer
    queue: asyncio.Queue[AccountEventPayload]
    name: str = "consumer"
    dropped: int = 0
    pump: asyncio.Task[None] | None = None
    on_loss: Callable[[int], None] | None = None
    #: Event currently dequeued into the consumer (loss accounting).
    in_flight: AccountEventPayload | None = None
    #: Per-subscription exit signal (self-unsubscribe: finish current, exit).
    stopped: bool = False


@dataclass(frozen=True)
class EventDispatcherStats:
    """Read-only dispatcher snapshot (milestone 5: queue pressure)."""

    subscriber_count: int
    total_dropped: int
    #: Cumulative lost events per subscribe-handle (overflow + abandoned).
    subscriber_dropped: Mapping[int, int]
    #: Live queue backlog per subscribe-handle.
    queue_depths: Mapping[int, int]


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

    def subscribe(
        self,
        consumer: EventConsumer,
        *,
        on_events_lost: Callable[[int], None] | None = None,
    ) -> Callable[[], None]:
        """Subscribe to normalized inbound events; returns unsubscribe.

        ``on_events_lost`` (optional) receives the subscriber's
        CUMULATIVE lost-event count whenever events are lost for it
        (overflow, failed consumer invocation, abandoned at stop or
        unsubscribe) — the reconciliation signal (§17.7).
        """
        ...

    async def get_channel(self, channel_id: int) -> ChannelInfo | None:
        """Return cached channel info, or ``None`` when not visible.

        Cache-only lookup (no platform API call): ``None`` means the
        account does not currently see the channel — an unstarted or
        reconnecting adapter has an empty cache, so ensure readiness
        first when that matters.
        """
        ...

    async def validate_channel(self, channel_id: int) -> bool:
        """True when *channel_id* is visible to the account (cache)."""
        ...

    async def close(self) -> None:
        """Release adapter resources. Must be safe to call more than once."""
        ...
